use std::collections::BinaryHeap;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use fs2::FileExt;
use futures_util::{StreamExt, TryStreamExt, stream::BoxStream};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path as ObjectPath;
use object_store::{
    ClientOptions, GetResult, ObjectStore, ObjectStoreExt, PutMode, PutOptions, RetryConfig,
    UpdateVersion,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const CONTROL_DIR: &str = ".varve";
const HEAD_FILE: &str = "head";
const LOCK_FILE: &str = "store.lock";
const TEMP_PREFIX: &str = ".varve-tmp-";
const RUNTIME_THREADS: usize = 2;
const FILE_HEAD_MAGIC: &[u8] = b"VARVE-FILE-HEAD\0\x01";
const FILE_HEAD_ID_LEN: usize = 16;
const FILE_HEAD_LENGTH_LEN: usize = 8;
const FILE_HEAD_CHECKSUM_LEN: usize = 32;
const FILE_HEAD_OVERHEAD: usize =
    FILE_HEAD_MAGIC.len() + FILE_HEAD_ID_LEN + FILE_HEAD_LENGTH_LEN + FILE_HEAD_CHECKSUM_LEN;
const MAX_HEAD_ENVELOPE_BYTES: usize = crate::wal::MAX_FRAME_BYTES + FILE_HEAD_OVERHEAD;
const S3_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const S3_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const S3_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const S3_MAX_RETRIES: usize = 3;

/// Maximum number of keys returned by one paginated listing call.
pub const MAX_LIST_PAGE_SIZE: usize = 1_000;
/// Maximum number of keys returned by the backward-compatible `list` API.
pub const MAX_LIST_KEYS: usize = 10_000;
/// Maximum number of keys accepted by one batch delete call.
pub const MAX_DELETE_BATCH_SIZE: usize = 1_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListPage {
    pub keys: Vec<String>,
    /// Exclusive cursor for the next call. Pass this value as `after` unchanged.
    pub next: Option<String>,
}

pub trait RemoteStore: Send + Sync {
    /// Filesystem adapters should expose their root so database paths cannot overlap object storage.
    fn local_root(&self) -> Option<&Path> {
        None
    }
    fn get(&self, key: &str) -> Result<Vec<u8>>;
    /// Reads an object and rejects bodies larger than `max_bytes`.
    ///
    /// The default validates the result after `get` returns. Custom adapters must override this
    /// method to guarantee a streaming allocation bound.
    fn get_bounded(&self, key: &str, max_bytes: usize) -> Result<Vec<u8>> {
        let bytes = self.get(key)?;
        ensure_size(bytes.len() as u64, max_bytes, "remote object")?;
        Ok(bytes)
    }
    fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()>;
    fn head(&self) -> Result<Option<HeadObject>>;
    fn compare_and_swap_head(&self, expected: Option<&str>, bytes: &[u8]) -> Result<String>;
    /// Lists every key under `prefix` in lexical order.
    ///
    /// This compatibility API fails once more than `MAX_LIST_KEYS` keys exist. Use `list_page`
    /// for large namespaces.
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
    /// Lists at most `limit` keys lexically after the exclusive `after` cursor.
    ///
    /// The default adapter calls `list`, so it is semantically compatible but not allocation-safe.
    /// Custom stores must override this method to bound provider-side and local materialization.
    fn list_page(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<ListPage> {
        validate_list_request(prefix, after, limit)?;
        page_from_ordered_keys(self.list(prefix)?, prefix, after, limit)
    }
    fn delete(&self, key: &str) -> Result<()>;
    /// Deletes at most `MAX_DELETE_BATCH_SIZE` validated keys.
    ///
    /// A returned error can mean that an earlier key was already deleted; callers must treat the
    /// whole batch as incomplete and retry idempotently.
    fn delete_batch(&self, keys: &[String]) -> Result<usize> {
        validate_delete_batch(keys)?;
        let mut deleted = 0;
        for key in keys {
            self.delete(key).with_context(|| {
                format!(
                    "remote batch delete failed after {deleted} of {} keys; some keys may already be deleted",
                    keys.len()
                )
            })?;
            deleted += 1;
        }
        Ok(deleted)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadObject {
    pub token: String,
    pub bytes: Vec<u8>,
}

pub struct FileStore {
    root: PathBuf,
    control: PathBuf,
}

impl std::fmt::Debug for FileStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileStore")
            .field("root", &self.root)
            .finish()
    }
}

impl FileStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let requested = root.as_ref();
        if requested.as_os_str().is_empty() {
            bail!("file store root must not be empty");
        }

        fs::create_dir_all(requested)
            .with_context(|| format!("create file store root {}", requested.display()))?;
        let root = requested
            .canonicalize()
            .with_context(|| format!("canonicalize file store root {}", requested.display()))?;
        if !root.is_dir() {
            bail!("file store root is not a directory: {}", root.display());
        }

        let control = root.join(CONTROL_DIR);
        match fs::symlink_metadata(&control) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!("file store control path must not be a symlink")
            }
            Ok(metadata) if !metadata.is_dir() => {
                bail!("file store control path is not a directory")
            }
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {
                fs::create_dir(&control).context("create file store control directory")?;
            }
            Err(error) => return Err(error).context("inspect file store control directory"),
        }

        let lock_path = control.join(LOCK_FILE);
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .context("create file store lock")?;
        lock.sync_all().context("sync file store lock")?;
        sync_dir(&control)?;
        sync_dir(&root)?;
        if let Some(parent) = root.parent() {
            sync_dir(parent)?;
        }

        Ok(Self { root, control })
    }

    fn lock(&self, exclusive: bool) -> Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.control.join(LOCK_FILE))
            .context("open file store lock")?;
        if exclusive {
            FileExt::lock_exclusive(&file).context("lock file store exclusively")?;
        } else {
            FileExt::lock_shared(&file).context("lock file store for reading")?;
        }
        Ok(file)
    }

    fn checked_path(&self, key: &str) -> Result<PathBuf> {
        validate_key(key, false)?;
        let mut path = self.root.clone();
        for component in key.split('/') {
            path.push(component);
            match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    bail!("object path contains a symlink: {key}")
                }
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).with_context(|| format!("inspect object path {key}"));
                }
            }
        }
        Ok(path)
    }

    fn ensure_parent(&self, key: &str) -> Result<PathBuf> {
        let components: Vec<_> = key.split('/').collect();
        let mut parent = self.root.clone();
        for component in &components[..components.len() - 1] {
            let next = parent.join(component);
            match fs::symlink_metadata(&next) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    bail!("object path contains a symlink: {key}")
                }
                Ok(metadata) if !metadata.is_dir() => {
                    bail!("object parent is not a directory: {key}")
                }
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    fs::create_dir(&next)
                        .with_context(|| format!("create object directory for {key}"))?;
                    sync_dir(&parent)?;
                }
                Err(error) => {
                    return Err(error).with_context(|| format!("inspect object parent for {key}"));
                }
            }
            parent = next;
        }
        Ok(parent)
    }

    fn read_head_unlocked(&self) -> Result<Option<HeadObject>> {
        let path = self.control.join(HEAD_FILE);
        let bytes =
            match read_file_bounded(&path, MAX_HEAD_ENVELOPE_BYTES, "file store head envelope") {
                Ok(bytes) => bytes,
                Err(error)
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == ErrorKind::NotFound) =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(error).context("read file store head"),
            };
        decode_versioned_head(&bytes).map(Some)
    }
}

impl RemoteStore for FileStore {
    fn local_root(&self) -> Option<&Path> {
        Some(&self.root)
    }
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.get_bounded(key, crate::wal::MAX_FRAME_BYTES)
    }

    fn get_bounded(&self, key: &str, max_bytes: usize) -> Result<Vec<u8>> {
        let _lock = self.lock(false)?;
        let path = self.checked_path(key)?;
        read_file_bounded(&path, max_bytes, &format!("remote object {key}"))
            .with_context(|| format!("read remote object {key}"))
    }

    fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        ensure_size(
            bytes.len() as u64,
            crate::wal::MAX_FRAME_BYTES,
            "immutable remote object",
        )?;
        let _lock = self.lock(true)?;
        let destination = self.checked_path(key)?;
        let parent = self.ensure_parent(key)?;
        let temporary = parent.join(format!("{TEMP_PREFIX}{}", Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .with_context(|| format!("create temporary object for {key}"))?;
        if let Err(error) = file.write_all(bytes).and_then(|()| file.sync_all()) {
            let _ = fs::remove_file(&temporary);
            return Err(error).with_context(|| format!("write immutable object {key}"));
        }
        drop(file);

        match fs::hard_link(&temporary, &destination) {
            Ok(()) => {
                sync_dir(&parent)?;
                fs::remove_file(&temporary)
                    .with_context(|| format!("remove temporary object for {key}"))?;
                sync_dir(&parent)?;
                Ok(())
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                fs::remove_file(&temporary)
                    .with_context(|| format!("remove collided temporary object for {key}"))?;
                sync_dir(&parent)?;
                let existing = read_file_bounded(
                    &destination,
                    bytes.len(),
                    &format!("immutable collision at {key}"),
                )?;
                if existing == bytes {
                    Ok(())
                } else {
                    bail!("immutable object collision at {key}")
                }
            }
            Err(error) => {
                let _ = fs::remove_file(&temporary);
                Err(error).with_context(|| format!("publish immutable object {key}"))
            }
        }
    }

    fn head(&self) -> Result<Option<HeadObject>> {
        let _lock = self.lock(false)?;
        self.read_head_unlocked()
    }

    fn compare_and_swap_head(&self, expected: Option<&str>, bytes: &[u8]) -> Result<String> {
        ensure_size(
            bytes.len() as u64,
            crate::wal::MAX_FRAME_BYTES,
            "remote head payload",
        )?;
        let _lock = self.lock(true)?;
        let current = self.read_head_unlocked()?;
        let matches = match (expected, current.as_ref()) {
            (None, None) => true,
            (Some(expected), Some(current)) => expected == current.token,
            _ => false,
        };
        if !matches {
            bail!("remote head compare-and-swap conflict")
        }

        let (token, envelope) = encode_versioned_head(bytes);
        let temporary = self
            .control
            .join(format!("{TEMP_PREFIX}head-{}", Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .context("create temporary remote head")?;
        if let Err(error) = file.write_all(&envelope).and_then(|()| file.sync_all()) {
            let _ = fs::remove_file(&temporary);
            return Err(error).context("write temporary remote head");
        }
        drop(file);

        if let Err(error) = fs::rename(&temporary, self.control.join(HEAD_FILE)) {
            let _ = fs::remove_file(&temporary);
            return Err(error).context("publish remote head");
        }
        sync_dir(&self.control)?;
        Ok(token)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        list_all_bounded(self, prefix)
    }

    fn list_page(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<ListPage> {
        validate_list_request(prefix, after, limit)?;
        let _lock = self.lock(false)?;
        let start = if prefix.is_empty() {
            self.root.clone()
        } else {
            self.checked_path(prefix)?
        };
        match fs::symlink_metadata(&start) {
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Ok(ListPage {
                    keys: Vec::new(),
                    next: None,
                });
            }
            Err(error) => {
                return Err(error).with_context(|| format!("inspect list prefix {prefix}"));
            }
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!("list prefix contains a symlink: {prefix}")
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Ok(ListPage {
                    keys: Vec::new(),
                    next: None,
                });
            }
            Ok(_) => {}
        }

        let capacity = limit + 1;
        let mut smallest = BinaryHeap::with_capacity(capacity);
        let mut directories = vec![
            fs::read_dir(&start)
                .with_context(|| format!("list remote directory {}", start.display()))?,
        ];
        while let Some(entries) = directories.last_mut() {
            let Some(entry) = entries.next() else {
                directories.pop();
                continue;
            };
            let entry = entry.context("read remote directory entry")?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .context("inspect remote directory entry")?;
            let name = entry.file_name();
            let name = name
                .to_str()
                .context("remote object tree contains a non-UTF-8 name")?;
            if path == self.control || name.starts_with(TEMP_PREFIX) {
                continue;
            }
            if file_type.is_symlink() {
                bail!("remote object tree contains a symlink: {}", path.display())
            }
            if file_type.is_dir() {
                directories.push(
                    fs::read_dir(&path)
                        .with_context(|| format!("list remote directory {}", path.display()))?,
                );
                continue;
            }
            if !file_type.is_file() {
                bail!("unsupported remote object file type: {}", path.display())
            }

            let key = file_path_to_key(&self.root, &path)?;
            validate_key(&key, false).context("filesystem listing returned an unsafe key")?;
            if !key_is_within_prefix(&key, prefix) {
                bail!("filesystem listing escaped requested prefix {prefix}")
            }
            if after.is_some_and(|cursor| key.as_str() <= cursor) {
                continue;
            }
            if smallest.len() < capacity {
                smallest.push(key);
            } else if smallest.peek().is_some_and(|largest| key < *largest) {
                smallest.pop();
                smallest.push(key);
            }
        }

        let mut keys = smallest.into_vec();
        keys.sort();
        Ok(finish_page(keys, limit))
    }

    fn delete(&self, key: &str) -> Result<()> {
        let _lock = self.lock(true)?;
        let path = self.checked_path(key)?;
        match fs::remove_file(&path) {
            Ok(()) => {
                let parent = path.parent().context("remote object has no parent")?;
                sync_dir(parent)?;
                Ok(())
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).with_context(|| format!("delete remote object {key}")),
        }
    }
}

pub struct S3Store {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    runtime: RuntimeWorker,
}

impl std::fmt::Debug for S3Store {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3Store")
            .field("prefix", &self.prefix)
            .field("runtime_threads", &RUNTIME_THREADS)
            .finish_non_exhaustive()
    }
}

impl S3Store {
    pub fn from_env() -> Result<Self> {
        let bucket = required_env("VARVE_S3_BUCKET")?;
        let prefix = optional_env("VARVE_S3_PREFIX")?.unwrap_or_default();
        validate_key(&prefix, true).context("invalid VARVE_S3_PREFIX")?;

        let allow_http = match optional_env("VARVE_S3_ALLOW_HTTP")?.as_deref() {
            None | Some("false") => false,
            Some("true") => true,
            Some(_) => bail!("VARVE_S3_ALLOW_HTTP must be exactly true or false"),
        };

        // Each S3 request has a 30-second timeout; retries stop after 3 retries or 30 seconds.
        let client_options = ClientOptions::new()
            .with_timeout(S3_REQUEST_TIMEOUT)
            .with_connect_timeout(S3_CONNECT_TIMEOUT);
        let retry_config = RetryConfig {
            max_retries: S3_MAX_RETRIES,
            retry_timeout: S3_RETRY_TIMEOUT,
            ..Default::default()
        };
        let mut builder = AmazonS3Builder::from_env()
            .with_client_options(client_options)
            .with_retry(retry_config)
            .with_bucket_name(bucket)
            .with_allow_http(allow_http)
            .with_conditional_put(S3ConditionalPut::ETagMatch);
        if let Some(region) = optional_env("VARVE_S3_REGION")? {
            if region.is_empty() {
                bail!("VARVE_S3_REGION must not be empty")
            }
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = optional_env("VARVE_S3_ENDPOINT")? {
            if endpoint.is_empty() {
                bail!("VARVE_S3_ENDPOINT must not be empty")
            }
            if endpoint.starts_with("http://") {
                if !allow_http {
                    bail!("HTTP S3 endpoint requires VARVE_S3_ALLOW_HTTP=true")
                }
            } else if !endpoint.starts_with("https://") {
                bail!("VARVE_S3_ENDPOINT must be an absolute http:// or https:// URL")
            }
            builder = builder.with_endpoint(endpoint);
        }

        let store = builder.build().context("build S3 remote store")?;
        let runtime = RuntimeWorker::new()?;
        Ok(Self {
            store: Arc::new(store),
            prefix,
            runtime,
        })
    }

    /// Returns an isolated child namespace with the same provider credentials.
    /// Prefixes are organizational boundaries, not a substitute for bucket IAM.
    pub fn scoped(&self, child: &str) -> Result<Self> {
        validate_key(child, false).context("invalid S3 child prefix")?;
        let prefix = if self.prefix.is_empty() {
            child.to_owned()
        } else {
            format!("{}/{child}", self.prefix)
        };
        validate_key(&prefix, false).context("combined S3 prefix is invalid")?;
        Ok(Self {
            store: Arc::clone(&self.store),
            prefix,
            runtime: RuntimeWorker::new()?,
        })
    }

    fn path(&self, key: &str) -> Result<ObjectPath> {
        validate_key(key, false)?;
        self.internal_path(key)
    }

    fn internal_path(&self, key: &str) -> Result<ObjectPath> {
        let path = if self.prefix.is_empty() {
            key.to_owned()
        } else if key.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}/{key}", self.prefix)
        };
        ObjectPath::parse(path).context("construct S3 object path")
    }
}

impl RemoteStore for S3Store {
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.get_bounded(key, crate::wal::MAX_FRAME_BYTES)
    }

    fn get_bounded(&self, key: &str, max_bytes: usize) -> Result<Vec<u8>> {
        let path = self.path(key)?;
        let store = Arc::clone(&self.store);
        let key = key.to_owned();
        self.runtime.run(async move {
            let result = store
                .get(&path)
                .await
                .with_context(|| format!("get S3 object {key}"))?;
            read_s3_result_bounded(result, max_bytes, &format!("S3 object {key}")).await
        })
    }

    fn put_immutable(&self, key: &str, bytes: &[u8]) -> Result<()> {
        ensure_size(
            bytes.len() as u64,
            crate::wal::MAX_FRAME_BYTES,
            "immutable S3 object",
        )?;
        let path = self.path(key)?;
        let store = Arc::clone(&self.store);
        let payload = bytes.to_vec();
        let key = key.to_owned();
        self.runtime.run(async move {
            let options = PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            };
            match store.put_opts(&path, payload.clone().into(), options).await {
                Ok(_) => Ok(()),
                Err(object_store::Error::AlreadyExists { .. }) => {
                    let result = store
                        .get(&path)
                        .await
                        .with_context(|| format!("verify immutable S3 collision at {key}"))?;
                    let existing = read_s3_result_bounded(
                        result,
                        payload.len(),
                        &format!("immutable S3 collision at {key}"),
                    )
                    .await?;
                    if existing.as_slice() == payload.as_slice() {
                        Ok(())
                    } else {
                        bail!("immutable object collision at {key}")
                    }
                }
                Err(error) => Err(error).with_context(|| format!("put immutable S3 object {key}")),
            }
        })
    }

    fn head(&self) -> Result<Option<HeadObject>> {
        let path = self.internal_path(&format!("{CONTROL_DIR}/{HEAD_FILE}"))?;
        let store = Arc::clone(&self.store);
        self.runtime.run(async move {
            let result = match store.get(&path).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(error).context("get S3 remote head"),
            };
            let token = encode_s3_token(result.meta.e_tag.clone(), result.meta.version.clone())?;
            let wire =
                read_s3_result_bounded(result, MAX_HEAD_ENVELOPE_BYTES, "S3 remote head").await?;
            let bytes = decode_s3_head(wire)?;
            Ok(Some(HeadObject { token, bytes }))
        })
    }

    fn compare_and_swap_head(&self, expected: Option<&str>, bytes: &[u8]) -> Result<String> {
        ensure_size(
            bytes.len() as u64,
            crate::wal::MAX_FRAME_BYTES,
            "S3 remote head payload",
        )?;
        let path = self.internal_path(&format!("{CONTROL_DIR}/{HEAD_FILE}"))?;
        let store = Arc::clone(&self.store);
        // A fresh envelope prevents ETag ABA when GC releases an otherwise identical head.
        let (_, payload) = encode_versioned_head(bytes);
        let mode = match expected {
            None => PutMode::Create,
            Some(token) => {
                let token: S3Token =
                    serde_json::from_str(token).context("invalid opaque S3 head token")?;
                if token.e_tag.is_none() {
                    bail!("S3 head token has no ETag for conditional update")
                }
                PutMode::Update(UpdateVersion {
                    e_tag: token.e_tag,
                    version: token.version,
                })
            }
        };
        self.runtime.run(async move {
            let options = PutOptions {
                mode,
                ..Default::default()
            };
            let result = store
                .put_opts(&path, payload.into(), options)
                .await
                .context("conditionally publish S3 remote head")?;
            encode_s3_token(result.e_tag, result.version)
        })
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        list_all_bounded(self, prefix)
    }

    fn list_page(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<ListPage> {
        validate_list_request(prefix, after, limit)?;
        let prefix_path = self.internal_path(prefix)?;
        let offset = after.map(|cursor| self.internal_path(cursor)).transpose()?;
        let store = Arc::clone(&self.store);
        let configured_prefix = self.prefix.clone();
        let requested_prefix = prefix.to_owned();
        let cursor = after.map(str::to_owned);
        self.runtime.run(async move {
            let provider_prefix = if prefix_path.as_ref().is_empty() {
                None
            } else {
                Some(&prefix_path)
            };
            let locations: BoxStream<'static, object_store::Result<ObjectPath>> = match offset {
                Some(offset) => store
                    .list_with_offset(provider_prefix, &offset)
                    .map_ok(|object| object.location)
                    .boxed(),
                None => store
                    .list(provider_prefix)
                    .map_ok(|object| object.location)
                    .boxed(),
            };
            read_s3_list_page(
                locations,
                &configured_prefix,
                &requested_prefix,
                cursor.as_deref(),
                limit,
            )
            .await
            .with_context(|| format!("list S3 prefix {requested_prefix}"))
        })
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.path(key)?;
        let store = Arc::clone(&self.store);
        let key = key.to_owned();
        self.runtime.run(async move {
            match store.delete(&path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(error) => Err(error).with_context(|| format!("delete S3 object {key}")),
            }
        })
    }

    fn delete_batch(&self, keys: &[String]) -> Result<usize> {
        validate_delete_batch(keys)?;
        let paths = keys
            .iter()
            .map(|key| self.path(key))
            .collect::<Result<Vec<_>>>()?;
        let total = paths.len();
        let store = Arc::clone(&self.store);
        self.runtime.run(async move {
            let input_paths = paths.clone();
            let input = futures_util::stream::iter(
                input_paths
                    .into_iter()
                    .map(Ok::<ObjectPath, object_store::Error>),
            )
            .boxed();
            let mut results = store.delete_stream(input);
            let mut deleted = 0;
            while let Some(result) = results.next().await {
                let path = result.with_context(|| {
                    format!(
                        "S3 batch delete failed after {deleted} of {total} keys; some keys may already be deleted"
                    )
                })?;
                let expected = paths.get(deleted).ok_or_else(|| {
                    anyhow!("S3 batch delete returned more results than requested; deletion status is unknown")
                })?;
                if &path != expected {
                    bail!(
                        "S3 batch delete returned results out of order after {deleted} of {total} keys; deletion status is unknown"
                    )
                }
                deleted += 1;
            }
            if deleted != total {
                bail!(
                    "S3 batch delete returned only {deleted} of {total} results; deletion status is unknown"
                )
            }
            Ok(deleted)
        })
    }
}

fn validate_list_request(prefix: &str, after: Option<&str>, limit: usize) -> Result<()> {
    validate_key(prefix, true).context("invalid remote list prefix")?;
    if limit == 0 || limit > MAX_LIST_PAGE_SIZE {
        bail!("remote list page limit must be between 1 and {MAX_LIST_PAGE_SIZE}, got {limit}")
    }
    if let Some(cursor) = after {
        validate_key(cursor, false).context("invalid remote list cursor")?;
        if !cursor_is_within_prefix(cursor, prefix) {
            bail!("remote list cursor is outside prefix {prefix:?}")
        }
    }
    Ok(())
}

fn validate_delete_batch(keys: &[String]) -> Result<()> {
    if keys.len() > MAX_DELETE_BATCH_SIZE {
        bail!(
            "remote delete batch exceeds {MAX_DELETE_BATCH_SIZE}-key limit: {} keys",
            keys.len()
        )
    }
    for key in keys {
        validate_key(key, false).with_context(|| format!("invalid remote delete key {key:?}"))?;
    }
    Ok(())
}

fn key_is_within_prefix(key: &str, prefix: &str) -> bool {
    prefix.is_empty()
        || key
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn cursor_is_within_prefix(cursor: &str, prefix: &str) -> bool {
    prefix.is_empty() || cursor == prefix || key_is_within_prefix(cursor, prefix)
}

fn finish_page(mut keys: Vec<String>, limit: usize) -> ListPage {
    let next = (keys.len() > limit).then(|| keys[limit - 1].clone());
    keys.truncate(limit);
    ListPage { keys, next }
}

fn page_from_ordered_keys(
    listed: Vec<String>,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
) -> Result<ListPage> {
    let mut previous: Option<&str> = None;
    let mut keys = Vec::with_capacity(limit + 1);
    for key in &listed {
        validate_key(key, false).context("remote listing returned an unsafe key")?;
        if !key_is_within_prefix(key, prefix) {
            bail!("remote listing returned key outside prefix {prefix:?}: {key}")
        }
        if previous.is_some_and(|prior| key.as_str() <= prior) {
            bail!("remote listing is not in strict lexicographic order")
        }
        previous = Some(key);
        if after.is_some_and(|cursor| key.as_str() <= cursor) {
            continue;
        }
        if keys.len() <= limit {
            keys.push(key.clone());
        }
    }
    Ok(finish_page(keys, limit))
}

fn list_all_bounded(store: &dyn RemoteStore, prefix: &str) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let page = store.list_page(prefix, after.as_deref(), MAX_LIST_PAGE_SIZE)?;
        if page.keys.len() > MAX_LIST_PAGE_SIZE {
            bail!("remote adapter returned an oversized list page")
        }
        let mut prior = after.as_deref();
        for key in &page.keys {
            validate_key(key, false).context("remote adapter returned an unsafe list key")?;
            if !key_is_within_prefix(key, prefix) {
                bail!("remote adapter returned a key outside prefix {prefix:?}: {key}")
            }
            if prior.is_some_and(|cursor| key.as_str() <= cursor) {
                bail!("remote adapter returned a non-increasing list page")
            }
            prior = Some(key);
        }
        if keys.len().saturating_add(page.keys.len()) > MAX_LIST_KEYS {
            bail!("remote listing exceeds {MAX_LIST_KEYS}-key compatibility limit; use list_page")
        }
        let expected_next = page.keys.last();
        if let Some(next) = page.next.as_ref() {
            validate_key(next, false).context("remote adapter returned an unsafe list cursor")?;
            if expected_next != Some(next) {
                bail!("remote adapter returned a list cursor that is not the last page key")
            }
        }
        keys.extend(page.keys);
        match page.next {
            Some(next) => after = Some(next),
            None => return Ok(keys),
        }
    }
}

fn file_path_to_key(root: &Path, path: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(root)
        .context("remote object escaped file store root")?;
    let mut key = String::new();
    for part in relative.components() {
        let component = part
            .as_os_str()
            .to_str()
            .context("remote object path is not valid UTF-8")?;
        if !key.is_empty() {
            key.push('/');
        }
        key.push_str(component);
    }
    Ok(key)
}

fn s3_relative_key(configured_prefix: &str, path: &ObjectPath) -> Result<String> {
    let raw = path.as_ref();
    if configured_prefix.is_empty() {
        return Ok(raw.to_owned());
    }
    raw.strip_prefix(configured_prefix)
        .and_then(|suffix| suffix.strip_prefix('/'))
        .filter(|relative| !relative.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("S3 listing escaped configured prefix"))
}

async fn read_s3_list_page(
    mut locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    configured_prefix: &str,
    requested_prefix: &str,
    after: Option<&str>,
    limit: usize,
) -> Result<ListPage> {
    let mut previous_path: Option<ObjectPath> = None;
    let mut keys = Vec::with_capacity(limit + 1);
    while let Some(path) = locations.try_next().await.context("stream S3 listing")? {
        if previous_path
            .as_ref()
            .is_some_and(|previous| path <= *previous)
        {
            bail!(
                "S3 provider did not return strict lexicographic listing order; cursor pagination is unsafe"
            )
        }
        previous_path = Some(path.clone());
        let key = s3_relative_key(configured_prefix, &path)?;
        if key == CONTROL_DIR || key.starts_with(&format!("{CONTROL_DIR}/")) {
            continue;
        }
        validate_key(&key, false).context("S3 listing returned an unsafe key")?;
        if !key_is_within_prefix(&key, requested_prefix) {
            bail!("S3 listing returned key outside prefix {requested_prefix:?}: {key}")
        }
        if after.is_some_and(|cursor| key.as_str() <= cursor) {
            bail!("S3 provider returned a key at or before the exclusive cursor")
        }
        keys.push(key);
        if keys.len() == limit + 1 {
            break;
        }
    }
    Ok(finish_page(keys, limit))
}

struct RuntimeWorker {
    handle: tokio::runtime::Handle,
    shutdown: mpsc::SyncSender<()>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl RuntimeWorker {
    fn new() -> Result<Self> {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (shutdown_tx, shutdown_rx) = mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("varve-s3-runtime".to_owned())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(RUNTIME_THREADS)
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.to_string()));
                        return;
                    }
                };
                if ready_tx.send(Ok(runtime.handle().clone())).is_err() {
                    return;
                }
                let _ = shutdown_rx.recv();
            })
            .context("spawn S3 runtime owner")?;
        let handle = ready_rx
            .recv()
            .context("S3 runtime owner stopped during startup")?
            .map_err(|error| anyhow!("build bounded S3 runtime: {error}"))?;
        Ok(Self {
            handle,
            shutdown: shutdown_tx,
            thread: Mutex::new(Some(thread)),
        })
    }

    fn run<T, F>(&self, future: F) -> Result<T>
    where
        T: Send + 'static,
        F: Future<Output = Result<T>> + Send + 'static,
    {
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        drop(self.handle.spawn(async move {
            let _ = result_tx.send(future.await);
        }));
        result_rx
            .recv()
            .context("S3 runtime stopped before operation completed")?
    }
}

impl Drop for RuntimeWorker {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
        if let Ok(thread) = self.thread.get_mut()
            && let Some(thread) = thread.take()
        {
            let _ = thread.join();
        }
    }
}

#[derive(Deserialize, Serialize)]
struct S3Token {
    e_tag: Option<String>,
    version: Option<String>,
}

fn decode_s3_head(wire: Vec<u8>) -> Result<Vec<u8>> {
    if wire.starts_with(FILE_HEAD_MAGIC) {
        return Ok(decode_versioned_head(&wire)?.bytes);
    }
    // Existing S3 roots used the bare logical payload; their next CAS upgrades the wire envelope.
    ensure_size(
        wire.len() as u64,
        crate::wal::MAX_FRAME_BYTES,
        "legacy S3 remote head",
    )?;
    Ok(wire)
}

fn encode_s3_token(e_tag: Option<String>, version: Option<String>) -> Result<String> {
    if e_tag.as_deref().is_none_or(str::is_empty) {
        bail!("S3 did not return an ETag required for strict head CAS")
    }
    serde_json::to_string(&S3Token { e_tag, version }).context("encode opaque S3 head token")
}

fn encode_versioned_head(bytes: &[u8]) -> (String, Vec<u8>) {
    let identifier = Uuid::new_v4();
    let mut envelope = Vec::with_capacity(
        FILE_HEAD_MAGIC.len()
            + FILE_HEAD_ID_LEN
            + FILE_HEAD_LENGTH_LEN
            + bytes.len()
            + FILE_HEAD_CHECKSUM_LEN,
    );
    envelope.extend_from_slice(FILE_HEAD_MAGIC);
    envelope.extend_from_slice(identifier.as_bytes());
    envelope.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    envelope.extend_from_slice(bytes);
    let checksum = blake3::hash(&envelope);
    envelope.extend_from_slice(checksum.as_bytes());
    (format!("file:{identifier}"), envelope)
}

fn decode_versioned_head(envelope: &[u8]) -> Result<HeadObject> {
    let header_len = FILE_HEAD_MAGIC.len() + FILE_HEAD_ID_LEN + FILE_HEAD_LENGTH_LEN;
    let minimum_len = header_len + FILE_HEAD_CHECKSUM_LEN;
    if envelope.len() < minimum_len || !envelope.starts_with(FILE_HEAD_MAGIC) {
        bail!("file store head has an invalid envelope")
    }

    let identifier_at = FILE_HEAD_MAGIC.len();
    let identifier_end = identifier_at + FILE_HEAD_ID_LEN;
    let length_end = identifier_end + FILE_HEAD_LENGTH_LEN;
    let length_bytes: [u8; FILE_HEAD_LENGTH_LEN] = envelope[identifier_end..length_end]
        .try_into()
        .context("decode file store head length")?;
    let payload_len_u64 = u64::from_le_bytes(length_bytes);
    ensure_size(
        payload_len_u64,
        crate::wal::MAX_FRAME_BYTES,
        "file store head payload",
    )?;
    let payload_len = usize::try_from(payload_len_u64)
        .context("file store head payload length does not fit in memory")?;
    let payload_end = length_end
        .checked_add(payload_len)
        .context("file store head payload length overflow")?;
    let checksum_at = envelope.len() - FILE_HEAD_CHECKSUM_LEN;
    if payload_end != checksum_at {
        bail!("file store head payload length mismatch")
    }

    let expected_checksum = blake3::hash(&envelope[..checksum_at]);
    if envelope[checksum_at..] != expected_checksum.as_bytes()[..] {
        bail!("file store head checksum mismatch")
    }

    let identifier_bytes: [u8; FILE_HEAD_ID_LEN] = envelope[identifier_at..identifier_end]
        .try_into()
        .context("decode file store head identifier")?;
    let identifier = Uuid::from_bytes(identifier_bytes);
    Ok(HeadObject {
        token: format!("file:{identifier}"),
        bytes: envelope[length_end..payload_end].to_vec(),
    })
}

fn ensure_size(length: u64, max_bytes: usize, description: &str) -> Result<()> {
    let maximum = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    if length > maximum {
        bail!("{description} exceeds {max_bytes} byte limit: {length} bytes")
    }
    Ok(())
}

fn read_file_bounded(path: &Path, max_bytes: usize, description: &str) -> Result<Vec<u8>> {
    let mut file = File::open(path).with_context(|| format!("open {description}"))?;
    let expected_u64 = file
        .metadata()
        .with_context(|| format!("inspect {description}"))?
        .len();
    ensure_size(expected_u64, max_bytes, description)?;
    let expected = usize::try_from(expected_u64)
        .with_context(|| format!("{description} length does not fit in memory"))?;
    let read_limit = u64::try_from(max_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut bytes = Vec::with_capacity(expected);
    Read::by_ref(&mut file)
        .take(read_limit)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read {description}"))?;
    ensure_size(bytes.len() as u64, max_bytes, description)?;
    if bytes.len() != expected {
        bail!(
            "{description} length changed while reading: expected {expected} bytes, read {} bytes",
            bytes.len()
        )
    }
    Ok(bytes)
}

async fn read_s3_result_bounded(
    result: GetResult,
    max_bytes: usize,
    description: &str,
) -> Result<Vec<u8>> {
    let expected_u64 = result.meta.size;
    ensure_size(expected_u64, max_bytes, description)?;
    let expected = usize::try_from(expected_u64)
        .with_context(|| format!("{description} length does not fit in memory"))?;
    let mut bytes = Vec::with_capacity(expected);
    let mut stream = result.into_stream();
    while let Some(chunk) = stream
        .try_next()
        .await
        .with_context(|| format!("stream {description}"))?
    {
        let remaining = max_bytes.saturating_sub(bytes.len());
        if chunk.len() > remaining {
            let probe = remaining.saturating_add(1).min(chunk.len());
            bytes.extend_from_slice(&chunk[..probe]);
            ensure_size(bytes.len() as u64, max_bytes, description)?;
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() != expected {
        bail!(
            "{description} length mismatch: expected {expected} bytes, read {} bytes",
            bytes.len()
        )
    }
    Ok(bytes)
}

fn validate_key(key: &str, allow_empty: bool) -> Result<()> {
    if key.is_empty() {
        if allow_empty {
            return Ok(());
        }
        bail!("remote object key must not be empty")
    }
    if key.starts_with('/') {
        bail!("remote object key must be relative")
    }
    if key.contains('\\') {
        bail!("remote object key must use forward slashes")
    }
    if key.contains("://") || key.contains(':') || key.contains('?') || key.contains('#') {
        bail!("remote object key must not be a URI")
    }
    if key.contains('%') {
        bail!("remote object key must not contain percent escapes")
    }
    if key.chars().any(char::is_control) {
        bail!("remote object key contains a control character")
    }

    for (index, component) in key.split('/').enumerate() {
        if component.is_empty() || component == "." || component == ".." {
            bail!("remote object key contains an unsafe path component")
        }
        if (index == 0 && component == CONTROL_DIR) || component.starts_with(TEMP_PREFIX) {
            bail!("remote object key uses a reserved component")
        }
    }
    Ok(())
}

fn required_env(name: &str) -> Result<String> {
    match optional_env(name)? {
        Some(value) if !value.is_empty() => Ok(value),
        _ => bail!("{name} is required and must not be empty"),
    }
}

fn optional_env(name: &str) -> Result<Option<String>> {
    std::env::var_os(name)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| anyhow!("{name} contains non-UTF-8 data"))
        })
        .transpose()
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory for sync: {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory: {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct OversizedPager;

    impl RemoteStore for OversizedPager {
        fn get(&self, _key: &str) -> Result<Vec<u8>> {
            bail!("unused")
        }

        fn put_immutable(&self, _key: &str, _bytes: &[u8]) -> Result<()> {
            bail!("unused")
        }

        fn head(&self) -> Result<Option<HeadObject>> {
            bail!("unused")
        }

        fn compare_and_swap_head(&self, _expected: Option<&str>, _bytes: &[u8]) -> Result<String> {
            bail!("unused")
        }

        fn list(&self, _prefix: &str) -> Result<Vec<String>> {
            bail!("unused")
        }

        fn list_page(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<ListPage> {
            validate_list_request(prefix, after, limit)?;
            let start = after
                .map(|cursor| {
                    cursor
                        .rsplit_once('/')
                        .context("synthetic cursor has no separator")?
                        .1
                        .parse::<usize>()
                        .context("parse synthetic cursor")
                        .map(|index| index + 1)
                })
                .transpose()?
                .unwrap_or(0);
            let total = MAX_LIST_KEYS + 1;
            let end = start.saturating_add(limit).min(total);
            let keys = (start..end)
                .map(|index| format!("objects/{index:05}"))
                .collect::<Vec<_>>();
            let next =
                (end < total).then(|| keys.last().expect("non-empty synthetic page").clone());
            Ok(ListPage { keys, next })
        }

        fn delete(&self, _key: &str) -> Result<()> {
            bail!("unused")
        }
    }

    #[test]
    fn compatibility_list_helper_fails_above_total_cap() {
        let error = list_all_bounded(&OversizedPager, "objects").unwrap_err();
        assert!(
            error.to_string().contains("use list_page"),
            "unexpected compatibility-list error: {error:#}"
        );
    }

    #[test]
    fn scoped_s3_namespaces_isolate_data_and_heads_without_environment_changes() -> Result<()> {
        let parent = S3Store {
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: "base".into(),
            runtime: RuntimeWorker::new()?,
        };
        let left = parent.scoped("left")?;
        let right = parent.scoped("right")?;
        left.put_immutable("objects/a", b"one")?;
        assert_eq!(left.get_bounded("objects/a", 3)?, b"one");
        assert!(left.get_bounded("objects/a", 2).is_err());
        assert!(right.get("objects/a").is_err());
        left.compare_and_swap_head(None, b"left")?;
        assert_eq!(right.head()?, None);
        assert_eq!(parent.head()?, None);
        for bad in ["", "../other", "/root", "a//b", "s3://bucket", "a\\b"] {
            assert!(parent.scoped(bad).is_err(), "{bad}");
        }
        Ok(())
    }

    #[test]
    fn s3_head_envelopes_prevent_aba_and_migrate_legacy_payloads() -> Result<()> {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let store = S3Store {
            store: backend.clone(),
            prefix: "versioned".into(),
            runtime: RuntimeWorker::new()?,
        };
        let path = store.internal_path(&format!("{CONTROL_DIR}/{HEAD_FILE}"))?;
        let raw_store = backend.clone();
        let raw_path = path.clone();
        store.runtime.run(async move {
            raw_store.put(&raw_path, b"same".to_vec().into()).await?;
            Ok(())
        })?;
        let legacy = store.head()?.expect("legacy head");
        assert_eq!(legacy.bytes, b"same");
        let first = store.compare_and_swap_head(Some(&legacy.token), b"same")?;
        let raw_store = backend.clone();
        let raw_path = path.clone();
        let first_wire = store
            .runtime
            .run(async move { Ok(raw_store.get(&raw_path).await?.bytes().await?.to_vec()) })?;
        let second = store.compare_and_swap_head(Some(&first), b"same")?;
        let raw_store = backend.clone();
        let raw_path = path.clone();
        let second_wire = store
            .runtime
            .run(async move { Ok(raw_store.get(&raw_path).await?.bytes().await?.to_vec()) })?;
        assert!(first_wire.starts_with(FILE_HEAD_MAGIC));
        assert_ne!(
            first_wire, second_wire,
            "identical logical heads need fresh wire versions"
        );
        assert_ne!(first, second);
        assert_eq!(store.head()?.expect("versioned head").bytes, b"same");
        assert!(store.compare_and_swap_head(Some(&first), b"stale").is_err());
        let mut corrupt = second_wire;
        *corrupt.last_mut().expect("checksum byte") ^= 1;
        store.runtime.run(async move {
            backend.put(&path, corrupt.into()).await?;
            Ok(())
        })?;
        assert!(
            store.head().is_err(),
            "versioned head checksum corruption must fail closed"
        );
        assert!(encode_s3_token(Some(String::new()), None).is_err());
        Ok(())
    }

    #[test]
    fn in_memory_s3_pages_with_stale_cursors_and_bulk_deletes() -> Result<()> {
        let store = S3Store {
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: "base".into(),
            runtime: RuntimeWorker::new()?,
        };
        for key in ["objects/a", "objects/b", "objects/d", "objects/e"] {
            store.put_immutable(key, key.as_bytes())?;
        }

        let first = store.list_page("objects", None, 2)?;
        assert_eq!(first.keys, ["objects/a", "objects/b"]);
        assert_eq!(first.next.as_deref(), Some("objects/b"));
        let stale = store.list_page("objects", Some("objects/c"), 2)?;
        assert_eq!(stale.keys, ["objects/d", "objects/e"]);
        assert_eq!(stale.next, None);

        let deleted = ["objects/a", "objects/d"].map(str::to_owned);
        assert_eq!(store.delete_batch(&deleted)?, 2);
        assert_eq!(store.list("objects")?, ["objects/b", "objects/e"]);
        Ok(())
    }

    #[test]
    fn s3_page_stream_consumes_only_page_plus_one_keys() -> Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let yielded = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&yielded);
        let worker = RuntimeWorker::new()?;
        let page = worker.run(async move {
            let locations = futures_util::stream::iter(0..10_000)
                .map(|index| {
                    Ok::<_, object_store::Error>(ObjectPath::from(format!(
                        "base/objects/{index:05}"
                    )))
                })
                .inspect(move |_| {
                    observed.fetch_add(1, Ordering::SeqCst);
                })
                .boxed();
            read_s3_list_page(locations, "base", "objects", None, 3).await
        })?;
        assert_eq!(page.keys.len(), 3);
        assert_eq!(page.next.as_deref(), Some("objects/00002"));
        assert_eq!(yielded.load(Ordering::SeqCst), 4);
        Ok(())
    }

    #[test]
    fn s3_page_stream_rejects_nonlexicographic_provider_order() -> Result<()> {
        let worker = RuntimeWorker::new()?;
        let error = worker
            .run(async move {
                let locations = futures_util::stream::iter([
                    Ok::<_, object_store::Error>(ObjectPath::from("base/objects/b")),
                    Ok::<_, object_store::Error>(ObjectPath::from("base/objects/a")),
                ])
                .boxed();
                read_s3_list_page(locations, "base", "objects", None, 2).await
            })
            .unwrap_err();
        assert!(
            error.to_string().contains("strict lexicographic"),
            "unexpected ordering error: {error:#}"
        );
        Ok(())
    }

    #[test]
    fn runtime_worker_is_safe_inside_an_existing_tokio_runtime() -> Result<()> {
        let outer = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        outer.block_on(async {
            let worker = RuntimeWorker::new()?;
            let value = worker.run(async { Ok::<_, anyhow::Error>(42) })?;
            assert_eq!(value, 42);
            drop(worker);
            Ok(())
        })
    }
}
