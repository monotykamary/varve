use std::fs::{self, OpenOptions};
use std::sync::{Arc, Barrier, Mutex};

use anyhow::{Context, Result};
use tempfile::TempDir;
use uuid::Uuid;
use varve::remote::{
    FileStore, HeadObject, MAX_DELETE_BATCH_SIZE, MAX_LIST_PAGE_SIZE, RemoteStore, S3Store,
};

const DEFAULT_OBJECT_LIMIT: usize = 128 * 1024 * 1024;
const FILE_HEAD_ENVELOPE_OVERHEAD: usize = b"VARVE-FILE-HEAD\0\x01".len() + 16 + 8 + 32;

#[test]
fn immutable_put_get_collision_list_and_delete() -> Result<()> {
    let temporary = TempDir::new()?;
    let store = FileStore::new(temporary.path().join("remote"))?;

    store.put_immutable("objects/a", b"alpha")?;
    store.put_immutable("objects/a", b"alpha")?;
    let collision = store.put_immutable("objects/a", b"different");
    assert!(
        collision.is_err(),
        "different immutable bytes must conflict"
    );
    assert_eq!(store.get("objects/a")?, b"alpha");

    store.put_immutable("objects/nested/b", b"bravo")?;
    store.put_immutable("other/c", b"charlie")?;
    assert_eq!(
        store.list("objects")?,
        vec!["objects/a", "objects/nested/b"]
    );
    assert_eq!(
        store.list("")?,
        vec!["objects/a", "objects/nested/b", "other/c"]
    );

    store.delete("objects/a")?;
    assert!(store.get("objects/a").is_err());
    store.delete("objects/a")?;
    assert_eq!(store.list("objects")?, vec!["objects/nested/b"]);
    Ok(())
}

#[test]
fn paginated_listing_has_exclusive_stable_cursors() -> Result<()> {
    let temporary = TempDir::new()?;
    let store = FileStore::new(temporary.path().join("remote"))?;
    for key in [
        "objects/a",
        "objects/b",
        "objects/d",
        "objects/nested/e",
        "other/z",
    ] {
        store.put_immutable(key, key.as_bytes())?;
    }

    let first = store.list_page("objects", None, 2)?;
    assert_eq!(first.keys, ["objects/a", "objects/b"]);
    assert_eq!(first.next.as_deref(), Some("objects/b"));
    let second = store.list_page("objects", first.next.as_deref(), 2)?;
    assert_eq!(second.keys, ["objects/d", "objects/nested/e"]);
    assert_eq!(second.next, None);

    let stale = store.list_page("objects", Some("objects/c"), 2)?;
    assert_eq!(stale.keys, ["objects/d", "objects/nested/e"]);
    assert_eq!(stale.next, None);
    assert!(
        store
            .list_page("objects", Some("objects/nested/z"), 2)?
            .keys
            .is_empty()
    );

    let mut keys = Vec::new();
    let mut after = None;
    loop {
        let page = store.list_page("objects", after.as_deref(), 1)?;
        keys.extend(page.keys);
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(
        keys,
        ["objects/a", "objects/b", "objects/d", "objects/nested/e"]
    );
    Ok(())
}

#[test]
fn paginated_listing_and_batch_delete_validate_before_work() -> Result<()> {
    let temporary = TempDir::new()?;
    let store = FileStore::new(temporary.path().join("remote"))?;
    store.put_immutable("objects/a", b"a")?;
    store.put_immutable("objects/b", b"b")?;
    store.put_immutable("objects/c", b"c")?;

    for limit in [0, MAX_LIST_PAGE_SIZE + 1] {
        assert!(store.list_page("objects", None, limit).is_err());
    }
    assert_eq!(
        store.list_page("objects", Some("objects"), 1)?.keys,
        ["objects/a"]
    );
    for cursor in ["other/a", "../objects/a", ".varve/head"] {
        assert!(
            store.list_page("objects", Some(cursor), 1).is_err(),
            "accepted cursor {cursor:?}"
        );
    }

    let unsafe_batch = vec!["objects/c".to_owned(), "../escape".to_owned()];
    assert!(store.delete_batch(&unsafe_batch).is_err());
    assert_eq!(store.get("objects/c")?, b"c");
    let oversized = vec!["objects/a".to_owned(); MAX_DELETE_BATCH_SIZE + 1];
    assert!(store.delete_batch(&oversized).is_err());
    assert_eq!(store.get("objects/a")?, b"a");

    let batch = vec!["objects/a".to_owned(), "objects/b".to_owned()];
    assert_eq!(store.delete_batch(&batch)?, 2);
    assert_eq!(store.delete_batch(&batch)?, 2, "delete is idempotent");
    assert_eq!(store.list("objects")?, vec!["objects/c"]);
    assert_eq!(store.delete_batch(&[])?, 0);
    Ok(())
}

#[test]
fn immutable_collision_read_is_bounded() -> Result<()> {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("remote");
    let store = FileStore::new(&root)?;
    store.put_immutable("objects/a", b"alpha")?;

    OpenOptions::new()
        .write(true)
        .open(root.join("objects/a"))?
        .set_len(1024)?;
    let error = store.put_immutable("objects/a", b"alpha").unwrap_err();
    assert!(
        error.to_string().contains("immutable collision")
            && error.to_string().contains("exceeds 5 byte limit"),
        "unexpected collision error: {error:#}"
    );
    assert_eq!(fs::metadata(root.join("objects/a"))?.len(), 1024);
    Ok(())
}

#[test]
fn missing_object_fails_closed() -> Result<()> {
    let temporary = TempDir::new()?;
    let store = FileStore::new(temporary.path())?;
    let error = store.get("objects/missing").unwrap_err();
    assert!(error.to_string().contains("read remote object"));
    Ok(())
}

#[test]
fn bounded_get_rejects_configured_limit_and_default_cap() -> Result<()> {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("remote");
    let store = FileStore::new(&root)?;
    store.put_immutable("objects/small", b"123456789")?;

    let error = store.get_bounded("objects/small", 8).unwrap_err();
    assert!(
        format!("{error:#}").contains("exceeds 8 byte limit"),
        "unexpected bounded-get error: {error:#}"
    );

    let sparse_path = root.join("objects/sparse");
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&sparse_path)?
        .set_len((DEFAULT_OBJECT_LIMIT + 1) as u64)?;
    let error = store.get("objects/sparse").unwrap_err();
    assert!(
        format!("{error:#}").contains("exceeds 134217728 byte limit"),
        "unexpected default-get error: {error:#}"
    );
    Ok(())
}

#[test]
fn head_cas_create_update_conflict_and_reopen() -> Result<()> {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("remote");
    let store = FileStore::new(&root)?;

    assert_eq!(store.head()?, None);
    assert!(
        store
            .compare_and_swap_head(Some("not-a-token"), b"invalid")
            .is_err()
    );

    let first = store.compare_and_swap_head(None, b"first")?;
    assert_eq!(
        store.head()?,
        Some(HeadObject {
            token: first.clone(),
            bytes: b"first".to_vec(),
        })
    );
    assert!(store.compare_and_swap_head(None, b"replace").is_err());
    assert!(
        store
            .compare_and_swap_head(Some("stale"), b"replace")
            .is_err()
    );

    let second = store.compare_and_swap_head(Some(&first), b"second")?;
    assert_ne!(first, second);
    let third = store.compare_and_swap_head(Some(&second), b"first")?;
    assert_ne!(first, third, "an ABA content cycle must get a new token");
    assert!(
        store
            .compare_and_swap_head(Some(&first), b"stale-after-aba")
            .is_err()
    );
    drop(store);

    let reopened = FileStore::new(&root)?;
    let head = reopened.head()?.context("head must survive reopen")?;
    assert_eq!(head.token, third);
    assert_eq!(head.bytes, b"first");
    Ok(())
}

#[test]
fn head_rejects_oversized_declared_payload_and_sparse_envelope() -> Result<()> {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("remote");
    let store = FileStore::new(&root)?;
    store.compare_and_swap_head(None, b"small")?;
    let head_path = root.join(".varve/head");

    let mut envelope = fs::read(&head_path)?;
    let length_at = b"VARVE-FILE-HEAD\0\x01".len() + 16;
    envelope[length_at..length_at + 8]
        .copy_from_slice(&((DEFAULT_OBJECT_LIMIT + 1) as u64).to_le_bytes());
    let checksum_at = envelope.len() - 32;
    let checksum = blake3::hash(&envelope[..checksum_at]);
    envelope[checksum_at..].copy_from_slice(checksum.as_bytes());
    fs::write(&head_path, envelope)?;

    let error = store.head().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("file store head payload exceeds 134217728 byte limit"),
        "unexpected declared-length error: {error:#}"
    );

    OpenOptions::new()
        .write(true)
        .open(&head_path)?
        .set_len((DEFAULT_OBJECT_LIMIT + FILE_HEAD_ENVELOPE_OVERHEAD + 1) as u64)?;
    let error = store.head().unwrap_err();
    assert!(
        format!("{error:#}").contains("file store head envelope exceeds"),
        "unexpected envelope-length error: {error:#}"
    );
    Ok(())
}

#[test]
fn corrupted_head_fails_closed() -> Result<()> {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("remote");
    let store = FileStore::new(&root)?;
    store.compare_and_swap_head(None, b"valid")?;
    std::fs::write(root.join(".varve/head"), b"corrupt")?;
    assert!(store.head().is_err());
    assert!(
        store
            .compare_and_swap_head(None, b"must-not-replace-corruption")
            .is_err()
    );
    Ok(())
}

#[test]
fn head_cas_is_process_safe_between_store_instances() -> Result<()> {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("remote");
    let initial_store = FileStore::new(&root)?;
    let token = initial_store.compare_and_swap_head(None, b"initial")?;
    drop(initial_store);

    let barrier = Arc::new(Barrier::new(3));
    let mut threads = Vec::new();
    for bytes in [b"left".to_vec(), b"right".to_vec()] {
        let root = root.clone();
        let token = token.clone();
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || -> Result<bool> {
            let store = FileStore::new(root)?;
            barrier.wait();
            Ok(store.compare_and_swap_head(Some(&token), &bytes).is_ok())
        }));
    }
    barrier.wait();

    let successes = threads
        .into_iter()
        .map(|thread| thread.join().expect("CAS worker must not panic"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .filter(|success| *success)
        .count();
    assert_eq!(successes, 1, "exactly one concurrent CAS may publish");

    let final_bytes = FileStore::new(&root)?
        .head()?
        .context("head must exist")?
        .bytes;
    assert!(final_bytes == b"left" || final_bytes == b"right");
    Ok(())
}

#[test]
fn rejects_unsafe_keys_for_every_keyed_operation() -> Result<()> {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("remote");
    let store = FileStore::new(&root)?;
    let unsafe_keys = [
        "",
        "/absolute",
        "../escape",
        "a/../escape",
        "./relative",
        "a//b",
        "a/",
        "s3://bucket/key",
        "https://example.invalid/key",
        "a\\b",
        "C:\\absolute",
        "a/%2e%2e/b",
        "a?query",
        "a#fragment",
        ".varve/head",
        "a/.varve-tmp-hidden",
        "a\0b",
    ];

    for key in unsafe_keys {
        assert!(store.get(key).is_err(), "get accepted unsafe key {key:?}");
        assert!(
            store.get_bounded(key, 1).is_err(),
            "bounded get accepted unsafe key {key:?}"
        );
        assert!(
            store.put_immutable(key, b"x").is_err(),
            "put accepted unsafe key {key:?}"
        );
        assert!(
            store.delete(key).is_err(),
            "delete accepted unsafe key {key:?}"
        );
        if !key.is_empty() {
            assert!(
                store.list(key).is_err(),
                "list accepted unsafe prefix {key:?}"
            );
            assert!(
                store.list_page(key, None, 1).is_err(),
                "list_page accepted unsafe prefix {key:?}"
            );
            assert!(
                store.list_page("", Some(key), 1).is_err(),
                "list_page accepted unsafe cursor {key:?}"
            );
        }
        assert!(
            store.delete_batch(&[key.to_owned()]).is_err(),
            "delete_batch accepted unsafe key {key:?}"
        );
    }
    assert_eq!(store.list("")?, Vec::<String>::new());
    assert!(!temporary.path().join("escape").exists());
    Ok(())
}

#[cfg(unix)]
#[test]
fn rejects_symlink_escape() -> Result<()> {
    use std::os::unix::fs::symlink;

    let temporary = TempDir::new()?;
    let outside = temporary.path().join("outside");
    std::fs::create_dir(&outside)?;
    std::fs::write(outside.join("secret"), b"secret")?;
    let root = temporary.path().join("remote");
    let store = FileStore::new(&root)?;
    symlink(&outside, root.join("link"))?;

    assert!(store.get("link/secret").is_err());
    assert!(store.put_immutable("link/new", b"bad").is_err());
    assert!(store.list("").is_err());
    assert!(!outside.join("new").exists());
    Ok(())
}

struct DefaultBoundedAdapter;

impl RemoteStore for DefaultBoundedAdapter {
    fn get(&self, _key: &str) -> Result<Vec<u8>> {
        Ok(b"fallback".to_vec())
    }

    fn put_immutable(&self, _key: &str, _bytes: &[u8]) -> Result<()> {
        Ok(())
    }

    fn head(&self) -> Result<Option<HeadObject>> {
        Ok(None)
    }

    fn compare_and_swap_head(&self, _expected: Option<&str>, _bytes: &[u8]) -> Result<String> {
        Ok("unused".to_owned())
    }

    fn list(&self, _prefix: &str) -> Result<Vec<String>> {
        Ok(vec!["objects/a".to_owned(), "objects/c".to_owned()])
    }

    fn delete(&self, _key: &str) -> Result<()> {
        Ok(())
    }
}

#[test]
fn default_get_bounded_fallback_validates_custom_adapter_result() {
    let error = DefaultBoundedAdapter.get_bounded("object", 7).unwrap_err();
    assert!(error.to_string().contains("exceeds 7 byte limit"));
}

#[test]
fn default_list_page_fallback_is_semantic_but_not_allocation_safe() -> Result<()> {
    let first = DefaultBoundedAdapter.list_page("objects", None, 1)?;
    assert_eq!(first.keys, ["objects/a"]);
    assert_eq!(first.next.as_deref(), Some("objects/a"));
    let second = DefaultBoundedAdapter.list_page("objects", first.next.as_deref(), 1)?;
    assert_eq!(second.keys, ["objects/c"]);
    assert_eq!(second.next, None);
    Ok(())
}

struct PartialDeleteAdapter {
    deleted: Mutex<Vec<String>>,
}

impl RemoteStore for PartialDeleteAdapter {
    fn get(&self, _key: &str) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn put_immutable(&self, _key: &str, _bytes: &[u8]) -> Result<()> {
        Ok(())
    }

    fn head(&self) -> Result<Option<HeadObject>> {
        Ok(None)
    }

    fn compare_and_swap_head(&self, _expected: Option<&str>, _bytes: &[u8]) -> Result<String> {
        Ok("unused".to_owned())
    }

    fn list(&self, _prefix: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    fn delete(&self, key: &str) -> Result<()> {
        if key == "objects/b" {
            anyhow::bail!("injected delete failure")
        }
        self.deleted
            .lock()
            .expect("delete record lock poisoned")
            .push(key.to_owned());
        Ok(())
    }
}

#[test]
fn default_delete_batch_reports_partial_completion_as_error() {
    let store = PartialDeleteAdapter {
        deleted: Mutex::new(Vec::new()),
    };
    let keys = ["objects/a", "objects/b", "objects/c"].map(str::to_owned);
    let error = store.delete_batch(&keys).unwrap_err();
    assert!(
        format!("{error:#}").contains("failed after 1 of 3 keys"),
        "unexpected batch error: {error:#}"
    );
    assert_eq!(
        *store.deleted.lock().expect("delete record lock poisoned"),
        ["objects/a"]
    );
}

static LIVE_S3_ENV: Mutex<()> = Mutex::new(());

#[test]
#[ignore = "requires VARVE_LIVE_S3_TEST=true and explicit VARVE_S3_* configuration"]
fn live_s3_contract_uses_an_isolated_random_prefix() -> Result<()> {
    if std::env::var("VARVE_LIVE_S3_TEST").as_deref() != Ok("true") {
        anyhow::bail!("set VARVE_LIVE_S3_TEST=true to explicitly enable cloud access")
    }

    let _environment_lock = LIVE_S3_ENV
        .lock()
        .expect("live S3 environment lock poisoned");
    let store = S3Store::from_env()?.scoped(&format!("live-contract-{}", Uuid::new_v4()))?;

    assert_eq!(store.head()?, None);
    store.put_immutable("objects/a", b"alpha")?;
    store.put_immutable("objects/a", b"alpha")?;
    store.put_immutable("objects/b", b"bravo")?;
    store.put_immutable("objects/c", b"charlie")?;
    assert!(store.put_immutable("objects/a", b"different").is_err());
    assert_eq!(store.get("objects/a")?, b"alpha");
    let first_page = store.list_page("objects", None, 2)?;
    assert_eq!(first_page.keys, ["objects/a", "objects/b"]);
    assert_eq!(first_page.next.as_deref(), Some("objects/b"));
    assert_eq!(
        store
            .list_page("objects", first_page.next.as_deref(), 2)?
            .keys,
        ["objects/c"]
    );

    let first = store.compare_and_swap_head(None, b"first")?;
    assert!(store.compare_and_swap_head(None, b"conflict").is_err());
    let second = store.compare_and_swap_head(Some(&first), b"second")?;
    assert_ne!(first, second);
    assert_eq!(store.head()?.context("live head missing")?.bytes, b"second");

    let keys = ["objects/a", "objects/b", "objects/c"].map(str::to_owned);
    assert_eq!(store.delete_batch(&keys)?, 3);
    assert!(store.get("objects/a").is_err());
    assert_eq!(store.list("")?, Vec::<String>::new());
    Ok(())
}
