use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs;
use std::path::Path;
#[cfg(feature = "fault-injection")]
use std::process::Command;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;
use tempfile::TempDir;
use varve::remote::{FileStore, HeadObject, ListPage, RemoteStore};
use varve::{Config, Database, Row, StoredRow, TableConfig, segment};
#[cfg(feature = "fault-injection")]
use varve::{JobAlter, JobKind};

const MAX_TOKEN_BYTES: usize = 64 * 1024;

fn config() -> Config {
    Config {
        segment_rows: 1,
        compact_min_segments: 8,
        maintenance_interval_ms: 1_000,
        ..Config::default()
    }
}

fn table() -> TableConfig {
    TableConfig {
        shards: 1,
        window_us: 100,
        rollup_widths_us: Vec::new(),
        ..TableConfig::default()
    }
}

fn row(timestamp_us: i64, value: f64) -> Row {
    Row {
        timestamp_us,
        tenant: "tenant".into(),
        series: "cpu".into(),
        value,
        tags: BTreeMap::new(),
    }
}

fn initialize(root: &Path, remote: Arc<dyn RemoteStore>) -> Database {
    let db = Database::open_with_remote(root, config(), Some(remote)).unwrap();
    db.create_table("metrics", table()).unwrap();
    db.write("metrics", "first", vec![row(1, 1.0)], 1).unwrap();
    db
}

#[derive(Clone, Copy, Debug)]
enum TokenKind {
    Valid,
    Empty,
    Oversized,
}

#[derive(Clone, Copy, Debug)]
enum CasReport {
    Token(TokenKind),
    AmbiguousWithHead(TokenKind),
}

#[derive(Default)]
struct TokenState {
    reports: VecDeque<CasReport>,
    outer_to_inner: HashMap<String, String>,
    next_head: Option<(String, TokenKind)>,
    history: Vec<String>,
}

struct ScriptedTokenStore {
    inner: FileStore,
    state: Mutex<TokenState>,
}

impl ScriptedTokenStore {
    fn new(path: &Path) -> Arc<Self> {
        Arc::new(Self {
            inner: FileStore::new(path).unwrap(),
            state: Mutex::new(TokenState::default()),
        })
    }

    fn script(&self, reports: impl IntoIterator<Item = CasReport>) {
        let mut state = self.state.lock().unwrap();
        state.reports = reports.into_iter().collect();
        state.next_head = None;
    }

    fn valid_token(inner: &str) -> String {
        let mut token = format!("varve-version:{inner}|");
        let escapes = ['"', '\\', '\0', '\u{1}', '\n', '\r', '\t'];
        let mut index = 0;
        while token.len() < MAX_TOKEN_BYTES {
            token.push(escapes[index % escapes.len()]);
            index += 1;
        }
        assert_eq!(token.len(), MAX_TOKEN_BYTES);
        token
    }

    fn report_token(&self, inner: &str, kind: TokenKind) -> String {
        let token = match kind {
            TokenKind::Valid => Self::valid_token(inner),
            TokenKind::Empty => String::new(),
            TokenKind::Oversized => "x".repeat(MAX_TOKEN_BYTES + 1),
        };
        let mut state = self.state.lock().unwrap();
        if !token.is_empty() {
            state.outer_to_inner.insert(token.clone(), inner.to_owned());
        }
        state.history.push(token.clone());
        token
    }

    fn resolve_expected(&self, token: &str) -> Result<String> {
        if let Some(inner) = self
            .state
            .lock()
            .unwrap()
            .outer_to_inner
            .get(token)
            .cloned()
        {
            return Ok(inner);
        }
        token
            .strip_prefix("varve-version:")
            .and_then(|rest| rest.split_once('|').map(|(inner, _)| inner.to_owned()))
            .context("unknown reported remote token")
    }

    fn raw_head(&self) -> HeadObject {
        self.inner.head().unwrap().unwrap()
    }

    fn history(&self) -> Vec<String> {
        self.state.lock().unwrap().history.clone()
    }
}

impl RemoteStore for ScriptedTokenStore {
    fn local_root(&self) -> Option<&Path> {
        self.inner.local_root()
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.inner.get(key)
    }

    fn get_bounded(&self, key: &str, max_bytes: usize) -> Result<Vec<u8>> {
        self.inner.get_bounded(key, max_bytes)
    }

    fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.inner.put_immutable(key, bytes)
    }

    fn head(&self) -> Result<Option<HeadObject>> {
        let Some(head) = self.inner.head()? else {
            return Ok(None);
        };
        let kind = {
            let mut state = self.state.lock().unwrap();
            match state.next_head.take() {
                Some((inner, kind)) if inner == head.token => kind,
                Some(pending) => {
                    state.next_head = Some(pending);
                    TokenKind::Valid
                }
                None => TokenKind::Valid,
            }
        };
        Ok(Some(HeadObject {
            token: self.report_token(&head.token, kind),
            bytes: head.bytes,
        }))
    }

    fn compare_and_swap_head(&self, expected: Option<&str>, bytes: &[u8]) -> Result<String> {
        let expected = expected
            .map(|token| self.resolve_expected(token))
            .transpose()?;
        let inner = self
            .inner
            .compare_and_swap_head(expected.as_deref(), bytes)?;
        let report = self
            .state
            .lock()
            .unwrap()
            .reports
            .pop_front()
            .unwrap_or(CasReport::Token(TokenKind::Valid));
        match report {
            CasReport::Token(kind) => Ok(self.report_token(&inner, kind)),
            CasReport::AmbiguousWithHead(kind) => {
                self.state.lock().unwrap().next_head = Some((inner, kind));
                bail!("injected ambiguous successful CAS response")
            }
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }

    fn list_page(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<ListPage> {
        self.inner.list_page(prefix, after, limit)
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key)
    }

    fn delete_batch(&self, keys: &[String]) -> Result<usize> {
        self.inner.delete_batch(keys)
    }
}

#[test]
fn max_escape_heavy_versioned_tokens_ship_reopen_and_reject_stale_cas() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("local");
    let remote = ScriptedTokenStore::new(&temp.path().join("remote"));
    let db = initialize(&root, remote.clone());

    assert_eq!(db.ship().unwrap(), 2);
    let first = remote.head().unwrap().unwrap();
    assert_eq!(first.token.len(), MAX_TOKEN_BYTES);
    assert!(first.token.contains('"'));
    assert!(first.token.contains('\\'));
    assert!(first.token.chars().any(char::is_control));

    db.write("metrics", "second", vec![row(2, 2.0)], 2).unwrap();
    assert_eq!(db.ship().unwrap(), 3);
    let second = remote.head().unwrap().unwrap();
    assert_ne!(first.token, second.token);
    assert_eq!(second.token.len(), MAX_TOKEN_BYTES);
    drop(db);

    let reopened = Database::open_with_remote(&root, config(), Some(remote.clone())).unwrap();
    let rows = reopened.scan("metrics", None, None, None, None).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].row, row(1, 1.0));
    assert_eq!(rows[1].row, row(2, 2.0));
    assert_eq!(reopened.status().unwrap().remote_sequence, 3);
    drop(reopened);

    assert!(
        remote
            .compare_and_swap_head(Some(&first.token), &second.bytes)
            .is_err(),
        "the wrapper must map each reported token to its distinct FileStore version"
    );
    let valid_versions = remote
        .history()
        .into_iter()
        .filter(|token| token.len() == MAX_TOKEN_BYTES)
        .collect::<Vec<_>>();
    assert!(valid_versions.windows(2).any(|pair| pair[0] != pair[1]));
}

#[cfg(feature = "fault-injection")]
#[test]
fn audit_token_crash_worker() {
    let Ok(root) = std::env::var("VARVE_AUDIT_TOKEN_ROOT") else {
        return;
    };
    let remote_path = std::env::var("VARVE_AUDIT_TOKEN_REMOTE").unwrap();
    let remote = ScriptedTokenStore::new(Path::new(&remote_path));
    let db = initialize(Path::new(&root), remote);
    db.ship().unwrap();
}

#[cfg(feature = "fault-injection")]
#[test]
fn max_escape_heavy_token_recovers_exact_intent_after_post_cas_crash() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("local");
    let remote_path = temp.path().join("remote");
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "audit_token_crash_worker", "--nocapture"])
        .env("VARVE_AUDIT_TOKEN_ROOT", &root)
        .env("VARVE_AUDIT_TOKEN_REMOTE", &remote_path)
        .env("VARVE_FAILPOINT", "remote_head_published")
        .env_remove("VARVE_IO_FAILPOINT")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(86), "{output:?}");
    assert!(root.join("remote-publication.intent").exists());
    assert!(!root.join("remote-binding.json").exists());

    let remote = ScriptedTokenStore::new(&remote_path);
    let db = Database::open_with_remote(&root, config(), Some(remote.clone())).unwrap();
    let rows = db.scan("metrics", None, None, None, None).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row, row(1, 1.0));
    let status = db.status().unwrap();
    assert_eq!(status.remote_sequence, status.sequence);
    assert!(status.fenced.is_none());
    assert!(!root.join("remote-publication.intent").exists());
    assert!(root.join("remote-binding.json").exists());

    db.write("metrics", "after-crash", vec![row(2, 2.0)], 2)
        .unwrap();
    assert_eq!(db.ship().unwrap(), 3);
    assert_eq!(remote.head().unwrap().unwrap().token.len(), MAX_TOKEN_BYTES);
}

fn invalid_reports() -> [(&'static str, CasReport); 4] {
    [
        ("empty-success", CasReport::Token(TokenKind::Empty)),
        ("oversized-success", CasReport::Token(TokenKind::Oversized)),
        (
            "empty-ambiguous-head",
            CasReport::AmbiguousWithHead(TokenKind::Empty),
        ),
        (
            "oversized-ambiguous-head",
            CasReport::AmbiguousWithHead(TokenKind::Oversized),
        ),
    ]
}

#[test]
fn invalid_committed_tokens_fence_ship_and_reopen_exact_intent() {
    for (case, report) in invalid_reports() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("local");
        let remote = ScriptedTokenStore::new(&temp.path().join("remote"));
        remote.script([report]);
        let db = initialize(&root, remote.clone());
        let sequence = db.status().unwrap().sequence;

        let error = db.ship().unwrap_err();
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("remote publication fenced:"),
            "case {case}: {rendered}"
        );
        assert!(
            rendered.contains("invalid token"),
            "case {case}: {rendered}"
        );
        let status = db.status().unwrap();
        assert_eq!(status.sequence, sequence, "case {case}");
        assert!(status.fenced.is_some(), "case {case}");
        assert!(
            db.write("metrics", "must-be-fenced", vec![row(9, 9.0)], 9)
                .is_err(),
            "case {case}"
        );
        assert!(root.join("remote-publication.intent").exists());
        drop(db);

        remote.script([]);
        let recovered = Database::open_with_remote(&root, config(), Some(remote.clone())).unwrap();
        let status = recovered.status().unwrap();
        assert_eq!(status.remote_sequence, sequence, "case {case}");
        assert_eq!(status.sequence, sequence, "case {case}");
        assert!(status.fenced.is_none(), "case {case}");
        assert_eq!(
            recovered.scan("metrics", None, None, None, None).unwrap()[0].row,
            row(1, 1.0),
            "case {case}"
        );
        assert!(!root.join("remote-publication.intent").exists());
        assert_eq!(remote.head().unwrap().unwrap().token.len(), MAX_TOKEN_BYTES);
    }
}

#[test]
fn invalid_gc_lock_tokens_fence_immediately_and_reopen_resumes_vacuum() {
    assert_invalid_gc_tokens(false);
}

#[test]
fn invalid_gc_release_tokens_fence_immediately_and_reopen_exact_intent() {
    assert_invalid_gc_tokens(true);
}

fn assert_invalid_gc_tokens(at_release: bool) {
    for (case, report) in invalid_reports() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("local");
        let remote = ScriptedTokenStore::new(&temp.path().join("remote"));
        let db = initialize(&root, remote.clone());
        db.checkpoint().unwrap();
        db.ship().unwrap();
        remote
            .put_immutable("segments/audit-orphan.parquet", b"orphan")
            .unwrap();
        let mut reports = vec![CasReport::Token(TokenKind::Valid)];
        if at_release {
            reports.push(CasReport::Token(TokenKind::Valid));
        }
        reports.push(report);
        remote.script(reports);

        let error = db.vacuum_remote().unwrap_err();
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("remote publication fenced:") && rendered.contains("invalid token"),
            "case {case}: {rendered}"
        );
        assert!(db.status().unwrap().fenced.is_some(), "case {case}");
        assert!(
            db.write("metrics", "must-be-fenced", vec![row(9, 9.0)], 9)
                .is_err(),
            "case {case}"
        );
        let current: Value = serde_json::from_slice(&remote.raw_head().bytes).unwrap();
        if at_release {
            assert!(current["lock"].is_null(), "case {case}");
        } else {
            assert_eq!(current["lock"]["kind"], "gc", "case {case}");
        }
        assert!(root.join("remote-publication.intent").exists());
        drop(db);

        remote.script([]);
        let recovered = Database::open_with_remote(&root, config(), Some(remote.clone())).unwrap();
        assert!(recovered.status().unwrap().fenced.is_none(), "case {case}");
        assert_eq!(
            recovered.scan("metrics", None, None, None, None).unwrap()[0].row,
            row(1, 1.0),
            "case {case}"
        );
        recovered.vacuum_remote().unwrap();
        assert!(
            remote.get("segments/audit-orphan.parquet").is_err(),
            "case {case}"
        );
        let released: Value = serde_json::from_slice(&remote.raw_head().bytes).unwrap();
        assert!(released["lock"].is_null(), "case {case}");
    }
}

struct PartialDeleteStore {
    inner: FileStore,
    armed: AtomicBool,
    deletion: AtomicUsize,
}

impl PartialDeleteStore {
    fn new(path: &Path) -> Arc<Self> {
        Arc::new(Self {
            inner: FileStore::new(path).unwrap(),
            armed: AtomicBool::new(false),
            deletion: AtomicUsize::new(0),
        })
    }

    fn arm(&self) {
        self.deletion.store(0, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }

    fn raw_head(&self) -> HeadObject {
        self.inner.head().unwrap().unwrap()
    }
}

impl RemoteStore for PartialDeleteStore {
    fn local_root(&self) -> Option<&Path> {
        self.inner.local_root()
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.inner.get(key)
    }

    fn get_bounded(&self, key: &str, max_bytes: usize) -> Result<Vec<u8>> {
        self.inner.get_bounded(key, max_bytes)
    }

    fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.inner.put_immutable(key, bytes)
    }

    fn head(&self) -> Result<Option<HeadObject>> {
        self.inner.head()
    }

    fn compare_and_swap_head(&self, expected: Option<&str>, bytes: &[u8]) -> Result<String> {
        self.inner.compare_and_swap_head(expected, bytes)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }

    fn list_page(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<ListPage> {
        self.inner.list_page(prefix, after, limit)
    }

    fn delete(&self, key: &str) -> Result<()> {
        if self.armed.load(Ordering::SeqCst) && self.deletion.fetch_add(1, Ordering::SeqCst) == 1 {
            self.armed.store(false, Ordering::SeqCst);
            bail!("injected second remote deletion failure")
        }
        self.inner.delete(key)
    }
}

#[test]
fn partial_remote_gc_batch_is_idempotent_and_preserves_head_data_and_lock_safety() {
    let temp = TempDir::new().unwrap();
    let remote = PartialDeleteStore::new(&temp.path().join("remote"));
    let db = initialize(&temp.path().join("local"), remote.clone());
    db.checkpoint().unwrap();
    db.ship().unwrap();
    let protected_segments = remote.list("segments").unwrap();
    assert!(!protected_segments.is_empty());
    let protected_manifest: Value = serde_json::from_slice(&remote.raw_head().bytes).unwrap();
    let protected_manifest = protected_manifest["checkpoint"]["key"]
        .as_str()
        .unwrap()
        .to_owned();
    remote
        .put_immutable("segments/audit-orphan-a.parquet", b"orphan-a")
        .unwrap();
    remote
        .put_immutable("segments/audit-orphan-b.parquet", b"orphan-b")
        .unwrap();
    remote.arm();

    let error = db.vacuum_remote().unwrap_err();
    assert!(
        format!("{error:#}").contains("failed after 1 of 2 keys"),
        "{error:#}"
    );
    assert!(db.status().unwrap().fenced.is_none());
    assert_eq!(
        db.scan("metrics", None, None, None, None).unwrap()[0].row,
        row(1, 1.0)
    );
    assert!(remote.get(&protected_manifest).is_ok());
    for key in &protected_segments {
        assert!(remote.get(key).is_ok(), "protected segment {key}");
    }
    assert!(remote.get("segments/audit-orphan-a.parquet").is_err());
    assert!(remote.get("segments/audit-orphan-b.parquet").is_ok());
    let released_after_error: Value = serde_json::from_slice(&remote.raw_head().bytes).unwrap();
    assert!(released_after_error["lock"].is_null());

    assert_eq!(db.vacuum_remote().unwrap(), 1);
    assert!(remote.get("segments/audit-orphan-b.parquet").is_err());
    assert!(remote.get(&protected_manifest).is_ok());
    for key in &protected_segments {
        assert!(remote.get(key).is_ok(), "protected segment {key}");
    }
    let released_after_retry: Value = serde_json::from_slice(&remote.raw_head().bytes).unwrap();
    assert!(released_after_retry["lock"].is_null());
}

struct BlockingPutStore {
    inner: FileStore,
    armed: AtomicBool,
    started: Mutex<Option<mpsc::SyncSender<()>>>,
    released: Mutex<bool>,
    release_cv: Condvar,
}

impl BlockingPutStore {
    fn new(path: &Path) -> (Arc<Self>, mpsc::Receiver<()>) {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        (
            Arc::new(Self {
                inner: FileStore::new(path).unwrap(),
                armed: AtomicBool::new(true),
                started: Mutex::new(Some(started_tx)),
                released: Mutex::new(false),
                release_cv: Condvar::new(),
            }),
            started_rx,
        )
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.release_cv.notify_all();
    }
}

struct ReleaseOnDrop(Arc<BlockingPutStore>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl RemoteStore for BlockingPutStore {
    fn local_root(&self) -> Option<&Path> {
        self.inner.local_root()
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.inner.get(key)
    }

    fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        if self.armed.swap(false, Ordering::SeqCst) {
            if let Some(started) = self.started.lock().unwrap().take() {
                started.send(()).unwrap();
            }
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.release_cv.wait(released).unwrap();
            }
            bail!("injected blocked upload failure")
        }
        self.inner.put_immutable(key, bytes)
    }

    fn head(&self) -> Result<Option<HeadObject>> {
        self.inner.head()
    }

    fn compare_and_swap_head(&self, expected: Option<&str>, bytes: &[u8]) -> Result<String> {
        self.inner.compare_and_swap_head(expected, bytes)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key)
    }
}

fn directory_bytes(root: &Path) -> u64 {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                directory_bytes(&entry.path())
            } else {
                entry.metadata().unwrap().len()
            }
        })
        .sum()
}

fn segment_files(root: &Path) -> Vec<String> {
    let mut files = fs::read_dir(root.join("segments"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".parquet"))
        .collect::<Vec<_>>();
    files.sort();
    files
}

#[test]
fn tight_checkpoint_retries_clean_unpublished_segments_with_an_unrelated_pin() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("local");
    let db = Database::open(&root, config()).unwrap();
    db.create_table("pinned", table()).unwrap();
    db.write("pinned", "pinned-row", vec![row(1, 1.0)], 1)
        .unwrap();
    db.checkpoint().unwrap();
    db.create_table("target", table()).unwrap();
    let target_row = row(2, 2.0);
    let receipt = db
        .write("target", "acknowledged", vec![target_row.clone()], 2)
        .unwrap();
    let wal_bytes = db.status().unwrap().wal_bytes;
    let existing_segments = segment_files(&root);
    assert_eq!(existing_segments.len(), 1);
    drop(db);

    let probe = temp.path().join("one-row.parquet");
    segment::write(
        &probe,
        &[StoredRow {
            row: target_row.clone(),
            sequence: receipt.sequence,
            ordinal: 0,
        }],
    )
    .unwrap();
    let one_row_segment_bytes = fs::metadata(&probe).unwrap().len();
    let baseline_bytes = directory_bytes(&root);
    let tight_limit = baseline_bytes + one_row_segment_bytes;
    let tight = Config {
        max_disk_bytes: tight_limit,
        wal_max_bytes: baseline_bytes,
        ..config()
    };
    assert!(tight_limit > tight.wal_max_bytes);

    let (remote, upload_started) = BlockingPutStore::new(&temp.path().join("remote"));
    let db = Database::open_with_remote(&root, tight.clone(), Some(remote.clone())).unwrap();
    let shipping_db = db.clone();
    let shipping = std::thread::spawn(move || shipping_db.ship());
    upload_started
        .recv_timeout(Duration::from_secs(5))
        .expect("ship did not capture and pin the existing segment");
    let release = ReleaseOnDrop(remote.clone());
    assert_eq!(db.status().unwrap().active_snapshots, 1);

    for attempt in 1..=2 {
        let error = db.checkpoint().unwrap_err();
        assert!(
            format!("{error:#}").contains("disk admission budget"),
            "attempt {attempt}: {error:#}"
        );
        let status = db.status().unwrap();
        assert!(status.fenced.is_none(), "attempt {attempt}");
        assert_eq!(status.hot_rows, 1, "attempt {attempt}");
        assert_eq!(status.wal_bytes, wal_bytes, "attempt {attempt}");
        assert_eq!(status.disk_bytes, baseline_bytes, "attempt {attempt}");
        assert_eq!(segment_files(&root), existing_segments, "attempt {attempt}");
        assert_eq!(
            db.scan("target", None, None, None, None).unwrap()[0].row,
            target_row,
            "attempt {attempt}"
        );
    }

    remote.release();
    drop(release);
    assert!(shipping.join().unwrap().is_err());
    drop(db);

    let recovered_tight = Database::open(&root, tight.clone()).unwrap();
    assert_eq!(
        recovered_tight
            .scan("target", None, None, None, None)
            .unwrap()[0]
            .row,
        target_row
    );
    assert_eq!(recovered_tight.status().unwrap().hot_rows, 1);
    assert_eq!(segment_files(&root), existing_segments);
    drop(recovered_tight);

    let roomy = Config {
        max_disk_bytes: tight.max_disk_bytes + 1024 * 1024,
        ..tight
    };
    let recovered = Database::open(&root, roomy).unwrap();
    recovered.checkpoint().unwrap();
    let after = recovered.status().unwrap();
    assert_eq!(after.hot_rows, 0);
    assert_eq!(after.wal_bytes, 0);
    let final_segments = segment_files(&root);
    assert_eq!(final_segments.len(), existing_segments.len() + 1);
    let new_segment = final_segments
        .iter()
        .find(|name| !existing_segments.contains(name))
        .unwrap();
    assert_eq!(
        fs::metadata(root.join("segments").join(new_segment))
            .unwrap()
            .len(),
        one_row_segment_bytes
    );
    assert_eq!(
        recovered.scan("target", None, None, None, None).unwrap()[0].row,
        target_row
    );
}

#[cfg(feature = "fault-injection")]
#[test]
fn audit_job_runtime_fault_worker() {
    let Ok(root) = std::env::var("VARVE_AUDIT_JOB_ROOT") else {
        return;
    };
    let mode = std::env::var("VARVE_AUDIT_JOB_MODE").unwrap();
    let db = Database::open(&root, config()).unwrap();
    let committed = db.status().unwrap().sequence + 1;
    let error = match mode.as_str() {
        "alter" => db
            .alter_job(
                "audit_job",
                JobAlter {
                    interval_us: Some(25),
                    paused: Some(true),
                },
            )
            .unwrap_err(),
        "drop" => db.drop_job("audit_job").unwrap_err(),
        _ => panic!("unknown audit job mode"),
    };
    let rendered = format!("{error:#}");
    let expected = format!(
        "job mutation committed at sequence {committed}, but runtime journal publication failed; database fenced, reopen and inspect the durable definition"
    );
    assert!(rendered.contains(&expected), "{rendered}");
    let status = db.status().unwrap();
    assert_eq!(status.sequence, committed);
    assert!(
        status
            .fenced
            .as_deref()
            .is_some_and(|reason| reason.contains(&expected))
    );
    assert!(
        db.create_job("blocked_after_commit", JobKind::Checkpoint, 10)
            .is_err()
    );
}

#[cfg(feature = "fault-injection")]
#[test]
fn committed_job_runtime_failures_report_sequence_fence_and_recover_definitions() {
    for mode in ["alter", "drop"] {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("local");
        let db = Database::open(&root, config()).unwrap();
        let created = db
            .create_job("audit_job", JobKind::Checkpoint, 100)
            .unwrap();
        db.run_job_now("audit_job", 1).unwrap();
        assert!(
            db.jobs()
                .unwrap()
                .into_iter()
                .find(|job| job.name == "audit_job")
                .unwrap()
                .latest_run
                .is_some()
        );
        drop(db);

        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "audit_job_runtime_fault_worker", "--nocapture"])
            .env("VARVE_AUDIT_JOB_ROOT", &root)
            .env("VARVE_AUDIT_JOB_MODE", mode)
            .env(
                "VARVE_IO_FAILPOINT",
                "atomic_job-runtime.bin_before_write:enospc",
            )
            .env_remove("VARVE_FAILPOINT")
            .output()
            .unwrap();
        assert!(output.status.success(), "mode {mode}: {output:?}");

        let reopened = Database::open(&root, config()).unwrap();
        assert_eq!(
            reopened.status().unwrap().sequence,
            created + 1,
            "mode {mode}"
        );
        let job = reopened
            .jobs()
            .unwrap()
            .into_iter()
            .find(|job| job.name == "audit_job");
        match mode {
            "alter" => {
                let job = job.expect("altered definition must survive restart");
                assert_eq!(job.interval_us, 25);
                assert!(job.paused);
                assert_eq!(job.updated_sequence, created + 1);
                assert!(
                    job.latest_run.is_none(),
                    "stale runtime generation was applied"
                );
                assert_eq!(job.attempts, 0);
                assert!(!job.running);
            }
            "drop" => assert!(job.is_none(), "dropped definition reappeared"),
            _ => unreachable!(),
        }
    }
}
