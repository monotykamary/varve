//! Private immutable-dependency publication, separate from WAL/root publication.
use super::{injected_io, read_bounded, sync_dir};
use anyhow::{Context, Result, ensure};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};

/// Owns the caller's pins until every directory barrier has completed.
///
/// The anchor must already have a durable directory entry. Callers serialize
/// each `stage` with disk admission and pin its identity before calling it.
/// Admission remains held from the budget check through rename: staged bytes
/// become actual accounted disk usage before admission can be released. No
/// temporary survives a successful stage, and no later stage replaces an
/// existing immutable object. Pins must exclude deletion until this guard (or
/// its durable successor) is dropped.
pub(crate) struct DependencyBatch<P> {
    anchor: PathBuf,
    directories: BTreeMap<PathBuf, BTreeSet<String>>,
    protection: P,
    failed: bool,
    #[cfg(test)]
    probe: std::sync::Arc<std::sync::Mutex<Probe>>,
}

/// Only `DependencyBatch::finish` can construct this publication prerequisite.
pub(crate) struct DurableDependencies<P> {
    _protection: P,
}

impl<P> DependencyBatch<P> {
    pub(crate) fn new(anchor: &Path, protection: P) -> Self {
        Self {
            anchor: anchor.to_path_buf(),
            directories: BTreeMap::new(),
            protection,
            failed: false,
            #[cfg(test)]
            probe: Default::default(),
        }
    }

    pub(crate) fn protection_mut(&mut self) -> &mut P {
        &mut self.protection
    }

    /// Verify/re-sync an existing object, or admit/write/sync/rename a new one.
    /// Returns whether a new object was created. Directory durability is NOT
    /// established by this method; only consuming `finish` proves it.
    pub(crate) fn stage(
        &mut self,
        path: &Path,
        bytes: &[u8],
        verify: impl FnOnce(&[u8]) -> Result<()>,
        admit: impl FnOnce(u64) -> Result<()>,
    ) -> Result<bool> {
        ensure!(!self.failed, "dependency batch already failed");
        // Poison before all fallible work, including verification/admission.
        self.failed = true;
        let relative = path
            .strip_prefix(&self.anchor)
            .context("dependency is outside its durable anchor")?;
        ensure!(
            relative.components().next().is_some()
                && relative
                    .components()
                    .all(|c| matches!(c, Component::Normal(_))),
            "invalid dependency path"
        );
        let parent = path.parent().context("dependency requires a parent")?;
        let target = path
            .file_name()
            .and_then(|name| name.to_str())
            .context("dependency requires a UTF-8 filename")?;
        fs::create_dir_all(parent)?;
        // Include all ancestors even when already present. They may have been
        // created by an interrupted batch or another publisher. Children are
        // synced before their parent entries in finish().
        let mut directory = parent;
        loop {
            ensure!(
                fs::symlink_metadata(directory)?.file_type().is_dir(),
                "dependency directory is not an ordinary directory"
            );
            self.directories.entry(directory.to_path_buf()).or_default();
            if directory == self.anchor {
                break;
            }
            directory = directory.parent().context("missing dependency anchor")?;
        }
        self.directories
            .get_mut(parent)
            .expect("dependency parent tracked")
            .insert(target.to_owned());
        let created = if path.try_exists()? {
            ensure!(
                fs::symlink_metadata(path)?.file_type().is_file(),
                "dependency is not an ordinary file"
            );
            verify(&read_bounded(path, bytes.len())?)?;
            self.completed("verify", path);
            // Integrity alone is not durability: this can be a leftover from a
            // batch that failed after rename, or another unfinished publisher.
            self.boundary(&format!("atomic_{target}_before_sync"), path)?;
            File::open(path)?.sync_all()?;
            self.completed("file_sync", path);
            false
        } else {
            admit(bytes.len() as u64)?;
            self.boundary(&format!("atomic_{target}_before_create"), path)?;
            let temp = parent.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
            let result = (|| -> Result<()> {
                let mut file = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&temp)?;
                self.completed("create", path);
                self.boundary(&format!("atomic_{target}_before_write"), path)?;
                file.write_all(bytes)?;
                self.completed("write", path);
                self.boundary(&format!("atomic_{target}_before_sync"), path)?;
                file.sync_all()?;
                self.completed("file_sync", path);
                self.boundary(&format!("atomic_{target}_before_rename"), path)?;
                fs::rename(&temp, path)?;
                self.completed("rename", path);
                Ok(())
            })();
            if result.is_err() {
                let _ = fs::remove_file(&temp);
            }
            result?;
            true
        };
        self.failed = false;
        Ok(created)
    }

    pub(crate) fn finish(self) -> Result<DurableDependencies<P>> {
        ensure!(!self.failed, "dependency batch already failed");
        let mut directories = self.directories.iter().collect::<Vec<_>>();
        directories.sort_by_key(|(path, _)| std::cmp::Reverse(path.components().count()));
        for (directory, targets) in directories {
            // Preserve each old per-object failure hook, while coalescing the
            // actual directory syscall. A hook error cannot mint a proof.
            for target in targets {
                self.boundary(
                    &format!("atomic_{target}_before_dir_sync"),
                    &directory.join(target),
                )?;
            }
            self.boundary("dependency_before_dir_sync", directory)?;
            sync_dir(directory)?;
            self.completed("directory_sync", directory);
        }
        Ok(DurableDependencies {
            _protection: self.protection,
        })
    }

    fn boundary(&self, name: &str, path: &Path) -> Result<()> {
        #[cfg(test)]
        {
            let mut probe = self.probe.lock().unwrap();
            let index = probe.before.len();
            probe.before.push((name.to_owned(), path.to_path_buf()));
            if probe.fail_at == Some(index) {
                anyhow::bail!("injected dependency I/O failure at {name}");
            }
        }
        let _ = path;
        Ok(injected_io(name)?)
    }

    fn completed(&self, operation: &str, path: &Path) {
        #[cfg(test)]
        self.probe
            .lock()
            .unwrap()
            .completed
            .push((operation.to_owned(), path.to_path_buf()));
        let _ = (operation, path);
    }
}

#[cfg(test)]
#[derive(Default)]
struct Probe {
    before: Vec<(String, PathBuf)>,
    completed: Vec<(String, PathBuf)>,
    fail_at: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestPin(Arc<AtomicUsize>);

    impl TestPin {
        fn new(count: &Arc<AtomicUsize>) -> Self {
            count.fetch_add(1, Ordering::SeqCst);
            Self(count.clone())
        }
    }

    impl Drop for TestPin {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn exact(actual: &[u8]) -> Result<()> {
        ensure!(actual == b"abc", "immutable dependency mismatch");
        Ok(())
    }

    fn count(probe: &Probe, operation: &str) -> usize {
        probe
            .completed
            .iter()
            .filter(|(op, _)| op == operation)
            .count()
    }

    // A low-level root writer that requires the same typed prerequisite as
    // engine preparation. This is not a database open/recovery simulation.
    fn root_after_dependencies<P>(
        root: &Path,
        _dependencies: &DurableDependencies<P>,
    ) -> Result<()> {
        super::super::atomic_write(&root.join("test-root"), b"references")
    }

    #[test]
    fn dependency_batch_counts_and_protection_lifetime() -> Result<()> {
        for n in [1, 3] {
            let root = tempfile::tempdir()?;
            let pins = Arc::new(AtomicUsize::new(0));
            let mut batch = DependencyBatch::new(root.path(), TestPin::new(&pins));
            let probe = batch.probe.clone();
            for i in 0..n {
                assert!(batch.stage(
                    &root.path().join(format!("{i}.page")),
                    b"abc",
                    exact,
                    |_| Ok(())
                )?);
            }
            assert_eq!(pins.load(Ordering::SeqCst), 1);
            assert_eq!(count(&probe.lock().unwrap(), "directory_sync"), 0);
            assert!(!root.path().join("test-root").exists());
            let durable = batch.finish()?;
            assert_eq!(pins.load(Ordering::SeqCst), 1);
            root_after_dependencies(root.path(), &durable)?;
            let trace = probe.lock().unwrap();
            for operation in ["create", "write", "file_sync", "rename"] {
                assert_eq!(count(&trace, operation), n, "{operation}");
            }
            assert_eq!(count(&trace, "directory_sync"), 1);
            let first_dir = trace
                .completed
                .iter()
                .position(|(op, _)| op == "directory_sync")
                .unwrap();
            let last_rename = trace
                .completed
                .iter()
                .rposition(|(op, _)| op == "rename")
                .unwrap();
            assert!(last_rename < first_dir);
            drop(durable);
            assert_eq!(pins.load(Ordering::SeqCst), 0);
        }
        Ok(())
    }

    #[test]
    fn dependency_new_and_leftover_directory_parent_barriers() -> Result<()> {
        for preexisting in [false, true] {
            let root = tempfile::tempdir()?;
            if preexisting {
                // No sync: existence must not be mistaken for durability.
                fs::create_dir_all(root.path().join("a/deep"))?;
                fs::create_dir_all(root.path().join("b"))?;
            }
            let mut batch = DependencyBatch::new(root.path(), ());
            let probe = batch.probe.clone();
            for name in ["a/deep/one.page", "a/deep/two.page", "b/three.page"] {
                batch.stage(&root.path().join(name), b"abc", exact, |_| Ok(()))?;
            }
            let _durable = batch.finish()?;
            let trace = probe.lock().unwrap();
            assert_eq!(count(&trace, "file_sync"), 3);
            assert_eq!(count(&trace, "directory_sync"), 4);
            let dirs = trace
                .completed
                .iter()
                .filter(|(op, _)| op == "directory_sync")
                .map(|(_, path)| path.strip_prefix(root.path()).unwrap().to_path_buf())
                .collect::<Vec<_>>();
            assert!(
                dirs.iter().position(|p| p == Path::new("a/deep"))
                    < dirs.iter().position(|p| p == Path::new("a"))
            );
            assert_eq!(dirs.last().unwrap(), Path::new(""));
        }
        Ok(())
    }

    #[test]
    fn dependency_every_io_failure_prevents_root_and_releases_pins() -> Result<()> {
        // First collect the actual production boundary trace; then fail each
        // boundary individually, including every file, rename and parent sync.
        let root = tempfile::tempdir()?;
        let mut baseline = DependencyBatch::new(root.path(), ());
        let trace = baseline.probe.clone();
        for name in ["a/one.page", "a/two.page", "b/three.page"] {
            baseline.stage(&root.path().join(name), b"abc", exact, |_| Ok(()))?;
        }
        let _durable = baseline.finish()?;
        let boundary_count = trace.lock().unwrap().before.len();
        assert_eq!(boundary_count, 18);
        for fail_at in 0..boundary_count {
            let root = tempfile::tempdir()?;
            let pins = Arc::new(AtomicUsize::new(0));
            let mut batch = DependencyBatch::new(root.path(), TestPin::new(&pins));
            batch.probe.lock().unwrap().fail_at = Some(fail_at);
            let result = (|| -> Result<()> {
                for name in ["a/one.page", "a/two.page", "b/three.page"] {
                    let staged = batch.stage(&root.path().join(name), b"abc", exact, |_| Ok(()));
                    if let Err(error) = staged {
                        assert_eq!(pins.load(Ordering::SeqCst), 1);
                        // Ignoring a stage failure must not allow a proof.
                        assert!(batch.finish().is_err());
                        return Err(error);
                    }
                }
                let durable = batch.finish()?;
                root_after_dependencies(root.path(), &durable)
            })();
            assert!(result.is_err(), "boundary {fail_at}");
            assert!(
                !root.path().join("test-root").exists(),
                "boundary {fail_at}"
            );
            assert_eq!(pins.load(Ordering::SeqCst), 0);
            for directory in ["a", "b"] {
                let path = root.path().join(directory);
                if path.exists() {
                    assert!(
                        fs::read_dir(path)?.all(|e| !e
                            .unwrap()
                            .file_name()
                            .to_string_lossy()
                            .ends_with(".tmp"))
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn dependency_leftover_reuse_is_verified_and_resynced() -> Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("one.page");
        let mut first = DependencyBatch::new(root.path(), ());
        first.stage(&path, b"abc", exact, |_| Ok(()))?;
        // The old atomic per-object hook still fails after rename and before
        // directory durability. The final file is visible but not a proof.
        first.probe.lock().unwrap().fail_at = Some(4);
        let trace = first.probe.clone();
        assert!(first.finish().is_err());
        assert_eq!(
            trace.lock().unwrap().before[4].0,
            "atomic_one.page_before_dir_sync"
        );
        assert!(path.exists());
        let mut second = DependencyBatch::new(root.path(), ());
        let trace = second.probe.clone();
        assert!(!second.stage(&path, b"abc", exact, |_| panic!(
            "existing bytes already charged"
        ))?);
        let durable = second.finish()?;
        root_after_dependencies(root.path(), &durable)?;
        let trace = trace.lock().unwrap();
        assert_eq!(count(&trace, "verify"), 1);
        assert_eq!(count(&trace, "file_sync"), 1);
        assert_eq!(count(&trace, "directory_sync"), 1);
        assert_eq!(count(&trace, "create"), 0);
        assert_eq!(count(&trace, "rename"), 0);
        Ok(())
    }

    #[test]
    fn dependency_reuse_failures_and_corruption_poison_batch() -> Result<()> {
        // Reuse has three fallible I/O boundaries: file sync, per-object dir
        // hook, and the shared directory barrier itself.
        for fail_at in 0..3 {
            let root = tempfile::tempdir()?;
            let path = root.path().join("one.page");
            fs::write(&path, b"abc")?;
            let mut batch = DependencyBatch::new(root.path(), ());
            batch.probe.lock().unwrap().fail_at = Some(fail_at);
            let staged = batch.stage(&path, b"abc", exact, |_| panic!("existing"));
            if fail_at == 0 {
                assert!(staged.is_err());
                assert!(batch.stage(&path, b"abc", exact, |_| Ok(())).is_err());
            } else {
                staged?;
            }
            assert!(batch.finish().is_err());
        }
        for corrupt in [b"bad".as_slice(), b"abcd".as_slice(), b"ab".as_slice()] {
            let root = tempfile::tempdir()?;
            let path = root.path().join("one.page");
            fs::write(&path, corrupt)?;
            let mut batch = DependencyBatch::new(root.path(), ());
            let trace = batch.probe.clone();
            assert!(batch.stage(&path, b"abc", exact, |_| Ok(())).is_err());
            assert!(batch.finish().is_err());
            assert_eq!(fs::read(&path)?, corrupt);
            assert_eq!(count(&trace.lock().unwrap(), "file_sync"), 0);
        }
        Ok(())
    }

    #[test]
    fn dependency_overlapping_publishers_do_not_trust_foreign_rename() -> Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("one.page");
        let disk_admission = std::sync::Mutex::new(());
        let pins = Arc::new(AtomicUsize::new(0));
        let mut first = DependencyBatch::new(root.path(), TestPin::new(&pins));
        let mut second = DependencyBatch::new(root.path(), TestPin::new(&pins));
        {
            let _disk = disk_admission.lock().unwrap();
            first.stage(&path, b"abc", exact, |_| Ok(()))?;
        }
        let trace = second.probe.clone();
        {
            let _disk = disk_admission.lock().unwrap();
            assert!(!second.stage(&path, b"abc", exact, |_| panic!("existing"))?);
        }
        assert_eq!(pins.load(Ordering::SeqCst), 2);
        let durable = second.finish()?;
        first.probe.lock().unwrap().fail_at = Some(4);
        assert!(first.finish().is_err());
        assert_eq!(pins.load(Ordering::SeqCst), 1);
        root_after_dependencies(root.path(), &durable)?;
        assert_eq!(fs::read(&path)?, b"abc");
        assert_eq!(count(&trace.lock().unwrap(), "file_sync"), 1);
        assert_eq!(count(&trace.lock().unwrap(), "directory_sync"), 1);
        drop(durable);
        assert_eq!(pins.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[test]
    fn dependency_staged_bytes_stay_charged_across_admission_unlock() -> Result<()> {
        let root = tempfile::tempdir()?;
        let disk_admission = std::sync::Mutex::new(());
        let admit = |additional: u64| -> Result<()> {
            let used = fs::read_dir(root.path())?
                .try_fold(0u64, |n, e| -> Result<u64> { Ok(n + e?.metadata()?.len()) })?;
            ensure!(used + additional <= 6, "disk budget exhausted");
            Ok(())
        };
        let mut first = DependencyBatch::new(root.path(), ());
        let mut second = DependencyBatch::new(root.path(), ());
        {
            let _disk = disk_admission.lock().unwrap();
            first.stage(&root.path().join("one.page"), b"abc", exact, admit)?;
        }
        {
            let _disk = disk_admission.lock().unwrap();
            second.stage(&root.path().join("two.page"), b"abc", exact, admit)?;
            assert!(
                second
                    .stage(&root.path().join("three.page"), b"abc", exact, admit)
                    .is_err()
            );
        }
        assert!(second.finish().is_err());
        assert!(!root.path().join("three.page").exists());
        assert_eq!(fs::read_dir(root.path())?.count(), 2);
        let _durable = first.finish()?;
        Ok(())
    }

    #[test]
    fn dependency_empty_batch_and_invalid_paths() -> Result<()> {
        let root = tempfile::tempdir()?;
        let batch = DependencyBatch::new(root.path(), ());
        let trace = batch.probe.clone();
        let _durable = batch.finish()?;
        assert!(trace.lock().unwrap().completed.is_empty());
        for path in [
            root.path().to_path_buf(),
            root.path().join("../escape.page"),
        ] {
            let mut batch = DependencyBatch::new(root.path(), ());
            assert!(batch.stage(&path, b"abc", exact, |_| Ok(())).is_err());
            assert!(batch.finish().is_err());
        }
        Ok(())
    }
}
