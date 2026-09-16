use crate::engine::*;
use crate::model::{Config, FORMAT_VERSION};
use crate::remote::{MAX_LIST_PAGE_SIZE, RemoteStore};
use crate::wal;
use anyhow::{Context, Result, bail, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObjectRef {
    pub key: String,
    pub digest: String,
    pub bytes: u64,
}
impl ObjectRef {
    fn new(key: String, bytes: &[u8]) -> Self {
        Self {
            key,
            digest: blake3::hash(bytes).to_hex().to_string(),
            bytes: bytes.len() as u64,
        }
    }
    pub fn verify(&self, bytes: &[u8]) -> Result<()> {
        ensure!(
            bytes.len() as u64 == self.bytes
                && blake3::hash(bytes).to_hex().as_str() == self.digest,
            "remote object integrity failure: {}",
            self.key
        );
        Ok(())
    }
    fn validate(&self, prefix: &str, suffix: &str) -> Result<()> {
        ensure!(
            self.digest.len() == 64
                && self
                    .digest
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
            "invalid remote object digest"
        );
        ensure!(
            self.bytes > 0 && self.bytes <= wal::MAX_FRAME_BYTES as u64,
            "remote object exceeds size limit"
        );
        ensure!(
            self.key.starts_with(prefix)
                && self.key.ends_with(suffix)
                && !self.key.contains("..")
                && !self.key.contains('\\'),
            "invalid remote object reference"
        );
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemoteLock {
    pub kind: String,
    pub owner: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemoteHead {
    pub format_version: u32,
    pub database_id: String,
    pub owner: String,
    pub sequence: u64,
    pub checkpoint: ObjectRef,
    pub wal: Vec<ObjectRef>,
    pub lock: Option<RemoteLock>,
}
impl RemoteHead {
    fn parse(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() <= wal::MAX_FRAME_BYTES,
            "remote head exceeds size limit"
        );
        let head: Self = serde_json::from_slice(bytes)?;
        ensure!(
            head.format_version == FORMAT_VERSION,
            "unsupported remote head version"
        );
        uuid::Uuid::parse_str(&head.database_id)?;
        uuid::Uuid::parse_str(&head.owner)?;
        head.checkpoint.validate("manifests/", ".bin")?;
        ensure!(
            head.checkpoint.key == format!("manifests/{}.bin", head.checkpoint.digest),
            "manifest key/digest mismatch"
        );
        let mut keys = BTreeSet::new();
        for record in &head.wal {
            record.validate("wal/", ".wal")?;
            ensure!(keys.insert(&record.key), "duplicate remote WAL reference");
        }
        if let Some(lock) = &head.lock {
            ensure!(
                ["restore", "gc"].contains(&lock.kind.as_str()),
                "unknown remote lock kind"
            );
            uuid::Uuid::parse_str(&lock.owner)?;
        }
        Ok(head)
    }
}
#[derive(Serialize, Deserialize)]
struct Binding {
    token: String,
    head: RemoteHead,
}

const INTENT_MAGIC: &[u8; 8] = b"VARVEPI1";
const INTENT_FILE: &str = "remote-publication.intent";
const BINDING_FILE: &str = "remote-binding.json";
const BINDING_RESERVATION_FILE: &str = "remote-binding.reserve";
const MAX_REMOTE_TOKEN_BYTES: usize = 64 * 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicationIntent {
    format_version: u32,
    root_digest: String,
    database_id: String,
    owner: String,
    sequence: u64,
    expected_token: Option<String>,
    intended_head_digest: String,
    intended_head: String,
    binding_reservation_bytes: u64,
}

fn validate_token(token: &str) -> Result<()> {
    ensure!(
        !token.is_empty() && token.len() <= MAX_REMOTE_TOKEN_BYTES,
        "remote head token exceeds local binding limit"
    );
    Ok(())
}

fn root_digest(root: &Path) -> Result<String> {
    let canonical = fs::canonicalize(root)?;
    let root = canonical
        .to_str()
        .context("database root must be valid UTF-8 for publication recovery")?;
    Ok(blake3::hash(root.as_bytes()).to_hex().to_string())
}

fn intent_path(root: &Path) -> PathBuf {
    root.join(INTENT_FILE)
}

fn reservation_path(root: &Path) -> PathBuf {
    root.join(BINDING_RESERVATION_FILE)
}

fn encode_intent(intent: &PublicationIntent) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(intent)?;
    ensure!(
        payload.len() <= wal::MAX_FRAME_BYTES - 48,
        "publication intent exceeds format limit"
    );
    let mut frame = Vec::with_capacity(payload.len() + 48);
    frame.extend_from_slice(INTENT_MAGIC);
    frame.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    frame.extend_from_slice(&payload);
    frame.extend_from_slice(blake3::hash(&frame).as_bytes());
    Ok(frame)
}

fn decode_intent(bytes: &[u8]) -> Result<PublicationIntent> {
    ensure!(
        bytes.len() >= 48 && bytes.len() <= wal::MAX_FRAME_BYTES,
        "invalid publication intent size"
    );
    ensure!(
        &bytes[..8] == INTENT_MAGIC,
        "invalid publication intent magic"
    );
    let payload_len = u64::from_le_bytes(bytes[8..16].try_into()?);
    ensure!(
        payload_len == (bytes.len() - 48) as u64,
        "truncated or trailing publication intent bytes"
    );
    let (content, digest) = bytes.split_at(bytes.len() - 32);
    ensure!(
        blake3::hash(content).as_bytes() == digest,
        "publication intent checksum mismatch"
    );
    let intent: PublicationIntent = serde_json::from_slice(&bytes[16..bytes.len() - 32])?;
    ensure!(
        intent.format_version == FORMAT_VERSION,
        "unsupported publication intent version"
    );
    if let Some(token) = intent.expected_token.as_deref() {
        validate_token(token)?;
    }
    ensure!(
        intent.intended_head.len() <= wal::MAX_FRAME_BYTES,
        "publication intent head exceeds size limit"
    );
    ensure!(
        blake3::hash(intent.intended_head.as_bytes())
            .to_hex()
            .as_str()
            == intent.intended_head_digest,
        "publication intent head checksum mismatch"
    );
    Ok(intent)
}

fn read_intent(root: &Path) -> Result<Option<PublicationIntent>> {
    let path = intent_path(root);
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(decode_intent(&wal::read_bounded(
        &path,
        wal::MAX_FRAME_BYTES,
    )?)?))
}

fn validate_intent(root: &Path, intent: &PublicationIntent, s: &State) -> Result<RemoteHead> {
    ensure!(
        intent.root_digest == root_digest(root)?,
        "publication intent belongs to a different database root"
    );
    ensure!(
        intent.database_id == s.catalog.database_id,
        "publication intent database identity mismatch"
    );
    ensure!(
        intent.sequence <= s.sequence,
        "publication intent sequence is ahead of local recovery state"
    );
    let head = RemoteHead::parse(intent.intended_head.as_bytes())?;
    ensure!(
        head.database_id == intent.database_id
            && head.owner == intent.owner
            && head.sequence == intent.sequence,
        "publication intent identity does not match its intended head"
    );
    ensure!(
        intent.binding_reservation_bytes > 0
            && intent.binding_reservation_bytes <= wal::MAX_FRAME_BYTES as u64,
        "invalid publication binding reservation size"
    );
    Ok(head)
}

fn remove_pending_publication(root: &Path) -> Result<()> {
    for path in [intent_path(root), reservation_path(root)] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    wal::sync_dir(root)
}

fn install_binding_from_reservation(
    root: &Path,
    token: String,
    head: RemoteHead,
    intent: &PublicationIntent,
) -> Result<()> {
    validate_token(&token)?;
    let bytes = serde_json::to_vec(&Binding { token, head })?;
    ensure!(
        bytes.len() as u64 <= intent.binding_reservation_bytes,
        "remote binding exceeds its physical reservation"
    );
    let reservation =
        wal::ReservedFile::open(reservation_path(root), intent.binding_reservation_bytes)?;
    wal::publish_reserved(
        reservation,
        &root.join(BINDING_FILE),
        &bytes,
        "remote_binding",
    )?;
    Ok(())
}

fn binding_matches_intent(binding: &Binding, intent: &PublicationIntent) -> Result<bool> {
    Ok(serde_json::to_vec(&binding.head)? == intent.intended_head.as_bytes())
}

pub(crate) fn load_binding(
    root: &Path,
    s: &mut State,
    remote: Option<&dyn RemoteStore>,
) -> Result<()> {
    let binding_path = root.join(BINDING_FILE);
    let mut binding = if binding_path.exists() {
        let binding: Binding =
            serde_json::from_slice(&wal::read_bounded(&binding_path, wal::MAX_FRAME_BYTES)?)?;
        validate_token(&binding.token)?;
        let head = RemoteHead::parse(&serde_json::to_vec(&binding.head)?)?;
        ensure!(
            head.database_id == s.catalog.database_id && head.sequence <= s.sequence,
            "remote binding does not match local recovery state"
        );
        Some(Binding {
            token: binding.token,
            head,
        })
    } else {
        None
    };

    if let Some(intent) = read_intent(root)? {
        let intended_head = validate_intent(root, &intent, s)?;
        if let Some(existing) = binding.as_ref() {
            if binding_matches_intent(existing, &intent)? {
                let current = remote.map(RemoteStore::head).transpose()?.flatten();
                if current.as_ref().is_some_and(|current| {
                    current.token == existing.token
                        && current.bytes == intent.intended_head.as_bytes()
                }) {
                    remove_pending_publication(root)?;
                } else if remote.is_some() {
                    bail!("durable binding and publication intent disagree with the remote head");
                }
            } else {
                ensure!(
                    intent.expected_token.as_deref() == Some(existing.token.as_str()),
                    "publication intent expected token does not match durable binding"
                );
                ensure!(
                    intended_head.database_id == existing.head.database_id
                        && intended_head.owner == existing.head.owner
                        && intended_head.sequence >= existing.head.sequence,
                    "publication intent does not advance the durable binding"
                );
            }
        } else {
            ensure!(
                intent.expected_token.is_none(),
                "unbound publication intent unexpectedly has a prior token"
            );
        }

        if let Some(remote) = remote {
            let current = remote.head()?;
            let exact = current
                .as_ref()
                .is_some_and(|current| current.bytes == intent.intended_head.as_bytes());
            if exact {
                let current = current.expect("checked above");
                validate_token(&current.token)?;
                let binding_is_current = match binding.as_ref() {
                    Some(existing) if existing.token == current.token => {
                        binding_matches_intent(existing, &intent)?
                    }
                    _ => false,
                };
                if !binding_is_current {
                    install_binding_from_reservation(
                        root,
                        current.token.clone(),
                        intended_head.clone(),
                        &intent,
                    )?;
                    binding = Some(Binding {
                        token: current.token,
                        head: intended_head,
                    });
                }
                remove_pending_publication(root)?;
            } else {
                let unchanged = match (&intent.expected_token, current.as_ref()) {
                    (None, None) => true,
                    (Some(expected), Some(current)) => current.token == *expected,
                    _ => false,
                };
                if unchanged {
                    remove_pending_publication(root)?;
                } else {
                    s.fenced = Some(
                        "pending publication intent does not match the exact remote head".into(),
                    );
                }
            }
        }
    } else if reservation_path(root).exists() {
        fs::remove_file(reservation_path(root))?;
        wal::sync_dir(root)?;
    }

    if let Some(binding) = binding {
        s.remote_owner = binding.head.owner.clone();
        s.remote_token = Some(binding.token);
        s.remote_head = Some(binding.head);
    }
    Ok(())
}

fn save_binding(inner: &Inner, s: &mut State, token: String, head: RemoteHead) -> Result<()> {
    let intent = read_intent(&inner.root)?;
    let result = if let Some(intent) = intent.as_ref() {
        ensure!(
            intent.intended_head.as_bytes() == serde_json::to_vec(&head)?,
            "publication result does not match durable intent"
        );
        install_binding_from_reservation(&inner.root, token.clone(), head.clone(), intent)
    } else {
        validate_token(&token)?;
        let bytes = serde_json::to_vec(&Binding {
            token: token.clone(),
            head: head.clone(),
        })?;
        wal::atomic_write(&inner.root.join(BINDING_FILE), &bytes)
    };
    if let Err(error) = result {
        s.fenced = Some(format!(
            "remote publication succeeded but local binding failed: {error:#}"
        ));
        return Err(error);
    }
    s.remote_token = Some(token);
    s.remote_head = Some(head);
    if intent.is_some()
        && let Err(error) = remove_pending_publication(&inner.root)
    {
        s.fenced = Some(format!(
            "remote binding is durable but publication intent cleanup failed: {error:#}"
        ));
        return Err(error);
    }
    Ok(())
}

pub(crate) fn validate_remote_layout(root: &Path, remote: Option<&dyn RemoteStore>) -> Result<()> {
    if let Some(remote_root) = remote.and_then(RemoteStore::local_root) {
        ensure!(
            !root.starts_with(remote_root) && !remote_root.starts_with(root),
            "local database and filesystem remote roots must be disjoint"
        );
    }
    Ok(())
}

#[derive(Clone)]
struct RemoteState {
    database_id: String,
    owner: String,
    token: Option<String>,
    segment_ids: BTreeSet<String>,
}

impl RemoteState {
    fn capture(s: &State) -> Self {
        Self {
            database_id: s.catalog.database_id.clone(),
            owner: s.remote_owner.clone(),
            token: s.remote_token.clone(),
            segment_ids: s.remote_segment_ids.clone(),
        }
    }

    fn still_current(&self, s: &State) -> bool {
        self.database_id == s.catalog.database_id
            && self.owner == s.remote_owner
            && self.token == s.remote_token
    }
}

struct WalUpload {
    sequence: u64,
    bytes: Vec<u8>,
}

struct ShipSnapshot {
    remote_state: RemoteState,
    sequence: u64,
    checkpoint: Manifest,
    checkpoint_bytes: Vec<u8>,
    wal: Vec<WalUpload>,
    _pin: Pin,
}

struct PublishedHead {
    token: String,
    head: RemoteHead,
    segment_ids: BTreeSet<String>,
}

struct ReconcileSnapshot {
    remote_state: RemoteState,
    sequence: u64,
    catalog: Manifest,
}

enum CurrentHead {
    Match(Option<RemoteHead>),
    Mismatch(String),
}

fn binding_reservation_bytes(head: &RemoteHead) -> Result<u64> {
    let base = serde_json::to_vec(&Binding {
        token: String::new(),
        head: head.clone(),
    })?
    .len();
    // A one-byte control character expands to six JSON bytes (for example, \u0000).
    let reserved = MAX_REMOTE_TOKEN_BYTES
        .checked_mul(6)
        .and_then(|token_bytes| base.checked_add(token_bytes))
        .context("remote binding reservation overflow")?;
    ensure!(
        reserved <= wal::MAX_FRAME_BYTES,
        "remote binding exceeds local format limit"
    );
    Ok(reserved as u64)
}

fn prepare_publication_intent(
    inner: &Inner,
    state: &RemoteState,
    head: &RemoteHead,
) -> Result<Vec<u8>> {
    ensure!(
        !intent_path(&inner.root).exists() && !reservation_path(&inner.root).exists(),
        "pending publication intent must be reconciled before another remote CAS"
    );
    let head_bytes = serde_json::to_vec(head)?;
    ensure!(
        head_bytes.len() <= wal::MAX_FRAME_BYTES,
        "remote head exceeds size limit"
    );
    let intended_head =
        String::from_utf8(head_bytes.clone()).context("remote head is not UTF-8")?;
    let intent = PublicationIntent {
        format_version: FORMAT_VERSION,
        root_digest: root_digest(&inner.root)?,
        database_id: state.database_id.clone(),
        owner: head.owner.clone(),
        sequence: head.sequence,
        expected_token: state.token.clone(),
        intended_head_digest: blake3::hash(&head_bytes).to_hex().to_string(),
        intended_head,
        binding_reservation_bytes: binding_reservation_bytes(head)?,
    };
    ensure!(
        intent.database_id == head.database_id && state.database_id == head.database_id,
        "publication intent database identity mismatch"
    );
    if let Some(token) = intent.expected_token.as_deref() {
        validate_token(token)?;
    }
    let intent_bytes = encode_intent(&intent)?;
    let additional = (intent_bytes.len() as u64)
        .checked_add(intent.binding_reservation_bytes)
        .context("publication reservation size overflow")?;
    let _disk = inner
        .disk_admission
        .lock()
        .map_err(|_| anyhow::anyhow!("disk admission lock poisoned"))?;
    ensure_budget(inner, additional)?;
    let reservation = wal::reserve_file(
        &reservation_path(&inner.root),
        intent.binding_reservation_bytes,
        "remote_binding_reservation",
    )?;
    ensure!(
        reservation.capacity() == intent.binding_reservation_bytes,
        "physical binding reservation size mismatch"
    );
    if let Err(error) = wal::atomic_write(&intent_path(&inner.root), &intent_bytes) {
        remove_pending_publication(&inner.root)
            .context("clean failed remote publication preparation")?;
        return Err(error.context("persist remote publication intent"));
    }
    wal::failpoint("remote_publication_intent_persisted");
    Ok(head_bytes)
}

fn compare_and_swap_with_intent(
    inner: &Inner,
    state: &RemoteState,
    head: &RemoteHead,
) -> Result<String> {
    let outcome = try_compare_and_swap_with_intent(inner, state, head);
    if let Err(error) = &outcome
        && error.to_string().starts_with("remote publication fenced:")
    {
        let mut live = inner
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("database state poisoned after remote publication"))?;
        if state.still_current(&live) {
            live.fenced = Some(format!("{error:#}"));
        }
    }
    outcome
}

fn try_compare_and_swap_with_intent(
    inner: &Inner,
    state: &RemoteState,
    head: &RemoteHead,
) -> Result<String> {
    let remote = inner
        .remote
        .as_ref()
        .context("no remote store configured")?;
    let head_bytes = prepare_publication_intent(inner, state, head)?;
    match remote.compare_and_swap_head(state.token.as_deref(), &head_bytes) {
        Ok(token) => {
            validate_token(&token).context(
                "remote publication fenced: CAS committed but returned an invalid token; intent preserved",
            )?;
            Ok(token)
        }
        Err(cas_error) => {
            let current = match remote.head() {
                Ok(current) => current,
                Err(read_error) => {
                    bail!(
                        "remote publication fenced: CAS failed and its outcome is ambiguous; intent preserved: CAS={cas_error:#}; HEAD={read_error:#}"
                    )
                }
            };
            if let Some(current) = current.as_ref()
                && current.bytes == head_bytes
            {
                validate_token(&current.token).context(
                    "remote publication fenced: committed CAS outcome has an invalid token; intent preserved",
                )?;
                return Ok(current.token.clone());
            }
            let unchanged = match (state.token.as_deref(), current.as_ref()) {
                (None, None) => true,
                (Some(expected), Some(current)) => current.token == expected,
                _ => false,
            };
            if unchanged {
                remove_pending_publication(&inner.root)?;
                return Err(cas_error.context("remote CAS rejected intended head"));
            }
            bail!(
                "remote publication fenced: CAS failed and remote head is neither the expected predecessor nor the exact intended bytes; intent preserved: {cas_error:#}"
            )
        }
    }
}

fn capture_ship(inner: &Inner, s: &State) -> Result<ShipSnapshot> {
    inner
        .remote
        .as_ref()
        .context("no remote store configured")?;
    let checkpoint_bytes = wal::read_bounded(
        &inner.root.join("manifest.bin"),
        inner.config.metadata_max_bytes,
    )?;
    let checkpoint = decode_manifest(&checkpoint_bytes)?;
    ensure!(
        checkpoint.checkpoint_sequence == s.catalog.checkpoint_sequence,
        "local checkpoint moved unexpectedly"
    );
    ensure!(
        checkpoint.checkpoint_sequence <= s.sequence,
        "local checkpoint is ahead of WAL state"
    );
    let segment_ids = checkpoint
        .tables
        .values()
        .flat_map(|table| table.segments.iter().map(|segment| segment.id.clone()))
        .collect();
    let pin = Pin::new(inner, segment_ids)?;
    let mut wal = Vec::new();
    let mut wal_bytes = 0u64;
    if checkpoint.checkpoint_sequence < s.sequence {
        let first_wal_sequence = checkpoint
            .checkpoint_sequence
            .checked_add(1)
            .context("shipping WAL sequence overflow")?;
        for sequence in first_wal_sequence..=s.sequence {
            let bytes = wal::read_bounded(&wal::path(&inner.root, sequence), wal::MAX_FRAME_BYTES)?;
            let record = wal::decode(&bytes)?;
            ensure!(
                record.sequence == sequence,
                "WAL sequence mismatch during shipping"
            );
            wal_bytes = wal_bytes
                .checked_add(bytes.len() as u64)
                .context("shipping WAL size overflow")?;
            ensure!(
                wal_bytes <= inner.config.wal_max_bytes,
                "shipping WAL snapshot exceeds configured WAL budget"
            );
            wal.push(WalUpload { sequence, bytes });
        }
    }
    Ok(ShipSnapshot {
        remote_state: RemoteState::capture(s),
        sequence: s.sequence,
        checkpoint,
        checkpoint_bytes,
        wal,
        _pin: pin,
    })
}

fn current_head_remote(inner: &Inner, state: &RemoteState) -> Result<CurrentHead> {
    let remote = inner
        .remote
        .as_ref()
        .context("no remote store configured")?;
    let current = remote.head()?;
    match (current, state.token.as_deref()) {
        (None, None) => Ok(CurrentHead::Match(None)),
        (Some(current), Some(expected)) if current.token == expected => {
            let head = RemoteHead::parse(&current.bytes)?;
            if head.database_id != state.database_id || head.owner != state.owner {
                return Ok(CurrentHead::Mismatch(
                    "remote identity no longer matches the local binding".into(),
                ));
            }
            Ok(CurrentHead::Match(Some(head)))
        }
        _ => Ok(CurrentHead::Mismatch(
            "conditional head no longer matches the local binding".into(),
        )),
    }
}

fn cutoff_covers(local: Option<i64>, remote: Option<i64>) -> bool {
    remote.is_none_or(|remote| local.is_some_and(|local| local >= remote))
}

fn job_run_covers(local: &crate::model::JobRun, remote: &crate::model::JobRun) -> bool {
    if local.run_id != remote.run_id {
        return local.run_id > remote.run_id;
    }
    if local.attempt != remote.attempt {
        return local.attempt > remote.attempt;
    }
    remote.finished_us.is_none() || local == remote
}

fn prove_owned_remote_prefix(
    inner: &Inner,
    head: &RemoteHead,
    local: &ReconcileSnapshot,
) -> Result<BTreeSet<String>> {
    ensure!(
        head.database_id == local.remote_state.database_id
            && head.owner == local.remote_state.owner
            && head.lock.is_none()
            && head.sequence <= local.sequence,
        "remote head is not an owned unlocked local prefix candidate"
    );
    let remote = inner
        .remote
        .as_ref()
        .context("no remote store configured")?;
    let bytes = remote.get_bounded(&head.checkpoint.key, head.checkpoint.bytes as usize)?;
    head.checkpoint.verify(&bytes)?;
    let checkpoint = decode_manifest(&bytes)?;
    ensure!(
        checkpoint.database_id == head.database_id
            && checkpoint.checkpoint_sequence <= head.sequence,
        "remote checkpoint does not match the owned head"
    );
    let tail_len = u64::try_from(head.wal.len()).context("remote WAL tail length overflow")?;
    ensure!(
        head.sequence.checked_sub(checkpoint.checkpoint_sequence) == Some(tail_len),
        "remote WAL tail is not contiguous with its checkpoint"
    );
    if let Some(remote_stamp) = checkpoint.control_history.last() {
        ensure!(
            local
                .catalog
                .control_history
                .iter()
                .any(|local_stamp| local_stamp == remote_stamp),
            "remote control checkpoint is not in bounded local control history"
        );
    }
    let control_matches = |sequence: u64, digest: &str| {
        local
            .catalog
            .control_history
            .iter()
            .any(|stamp| stamp.sequence == sequence && stamp.digest == digest)
    };
    let mut tables: BTreeSet<String> = checkpoint.tables.keys().cloned().collect();
    let mut receipts = BTreeSet::new();
    for (name, remote_table) in &checkpoint.tables {
        let local_table = local
            .catalog
            .tables
            .get(name)
            .with_context(|| format!("remote table {name} is absent locally"))?;
        ensure!(
            local_table
                .creation_config
                .as_ref()
                .unwrap_or(&local_table.config)
                == remote_table
                    .creation_config
                    .as_ref()
                    .unwrap_or(&remote_table.config)
                && local_table.created_sequence == remote_table.created_sequence,
            "remote table {name} does not match immutable local metadata"
        );
        ensure!(
            cutoff_covers(local_table.cutoff_us, remote_table.cutoff_us)
                && cutoff_covers(local_table.rollup_cutoff_us, remote_table.rollup_cutoff_us),
            "local retention cutoffs do not cover remote table {name}"
        );
        for (request_id, remote_receipt) in &remote_table.receipts {
            ensure!(
                receipts.insert((name.clone(), request_id.clone())),
                "duplicate remote receipt"
            );
            let local_receipt = local_table
                .receipts
                .get(request_id)
                .with_context(|| format!("remote receipt {name}/{request_id} is absent locally"))?;
            ensure!(
                local_receipt.sequence == remote_receipt.sequence
                    && local_receipt.rows == remote_receipt.rows
                    && local_receipt.digest == remote_receipt.digest,
                "remote receipt {name}/{request_id} does not match local history"
            );
        }
    }
    for (name, remote_job) in &checkpoint.jobs {
        let local_job = local.catalog.jobs.get(name).with_context(|| {
            format!("remote job {name} is absent locally; bounded runtime proof unavailable")
        })?;
        ensure!(
            local_job.created_sequence == remote_job.created_sequence,
            "remote job {name} creation identity changed"
        );
        if let Some(remote_run) = &remote_job.latest_run {
            let local_run = local_job
                .latest_run
                .as_ref()
                .with_context(|| format!("remote job run {name} is absent locally"))?;
            ensure!(
                job_run_covers(local_run, remote_run),
                "remote job runtime {name} is not a local prefix"
            );
        }
    }
    let mut wal_bytes = 0u64;
    for (index, object) in head.wal.iter().enumerate() {
        let offset = u64::try_from(index)
            .context("remote WAL index overflow")?
            .checked_add(1)
            .context("remote WAL index overflow")?;
        let sequence = checkpoint
            .checkpoint_sequence
            .checked_add(offset)
            .context("remote WAL sequence overflow")?;
        let bytes = remote.get_bounded(&object.key, object.bytes as usize)?;
        object.verify(&bytes)?;
        wal_bytes = wal_bytes
            .checked_add(bytes.len() as u64)
            .context("remote WAL proof size overflow")?;
        ensure!(
            wal_bytes <= inner.config.wal_max_bytes,
            "remote WAL proof exceeds configured WAL budget"
        );
        let record = wal::decode(&bytes)?;
        ensure!(
            record.sequence == sequence
                && object.key == format!("wal/{sequence:020}-{}.wal", object.digest),
            "remote WAL sequence/key mismatch"
        );
        match record.operation {
            wal::Operation::CreateTable { name, config } => {
                ensure!(
                    tables.insert(name.clone()),
                    "duplicate remote table creation"
                );
                let local_table =
                    local.catalog.tables.get(&name).with_context(|| {
                        format!("remote-created table {name} is absent locally")
                    })?;
                ensure!(
                    local_table
                        .creation_config
                        .as_ref()
                        .unwrap_or(&local_table.config)
                        == &config
                        && local_table.created_sequence == sequence,
                    "remote-created table {name} does not match local history"
                );
            }
            wal::Operation::Append {
                table,
                request_id,
                digest,
                rows,
            } => {
                ensure!(
                    tables.contains(&table),
                    "remote WAL references unknown table"
                );
                ensure!(
                    receipts.insert((table.clone(), request_id.clone())),
                    "duplicate remote WAL receipt"
                );
                ensure!(
                    blake3::hash(&serde_json::to_vec(&rows)?).to_hex().as_str() == digest,
                    "remote WAL payload digest mismatch"
                );
                let local_receipt = local
                    .catalog
                    .tables
                    .get(&table)
                    .and_then(|table| table.receipts.get(&request_id))
                    .with_context(|| {
                        format!("remote WAL receipt {table}/{request_id} is absent locally")
                    })?;
                ensure!(
                    local_receipt.sequence == sequence
                        && local_receipt.rows == rows.len()
                        && local_receipt.digest == digest,
                    "remote WAL receipt {table}/{request_id} does not match local history"
                );
            }
            wal::Operation::SetPolicy { stamp, .. }
            | wal::Operation::CreateContinuousAggregate { stamp, .. }
            | wal::Operation::DropContinuousAggregate { stamp, .. }
            | wal::Operation::PutJob { stamp, .. }
            | wal::Operation::DropJob { stamp, .. } => {
                ensure!(
                    control_matches(sequence, &stamp),
                    "remote control mutation is absent from local history"
                );
            }
            wal::Operation::JobStarted { name, run, .. }
            | wal::Operation::JobFinished { name, run, .. } => {
                let local_job = local.catalog.jobs.get(&name).with_context(|| {
                    format!("remote job runtime {name} has no bounded local identity proof")
                })?;
                let local_run = local_job
                    .latest_run
                    .as_ref()
                    .context("remote job run is absent locally")?;
                ensure!(
                    job_run_covers(local_run, &run),
                    "remote job runtime is not a local prefix"
                );
            }
        }
    }
    for (name, local_table) in &local.catalog.tables {
        if local_table.created_sequence <= head.sequence {
            ensure!(
                tables.contains(name),
                "local table {name} is missing from the remote prefix"
            );
        }
        for (request_id, receipt) in &local_table.receipts {
            if receipt.sequence <= head.sequence {
                ensure!(
                    receipts.contains(&(name.clone(), request_id.clone())),
                    "local receipt {name}/{request_id} is missing from the remote prefix"
                );
            }
        }
    }
    Ok(checkpoint
        .tables
        .values()
        .flat_map(|table| table.segments.iter().map(|segment| segment.id.clone()))
        .collect())
}

fn reconcile_owned_head(db: &Database) -> Result<()> {
    let snapshot = {
        let s = db.lock()?;
        healthy(&s)?;
        ReconcileSnapshot {
            remote_state: RemoteState::capture(&s),
            sequence: s.sequence,
            catalog: s.catalog.clone(),
        }
    };
    let remote = db
        .inner
        .remote
        .as_ref()
        .context("no remote store configured")?;
    let Some(current) = remote.head()? else {
        if snapshot.remote_state.token.is_some() {
            let reason = "bound remote head is missing";
            fence_remote(db, &snapshot.remote_state, reason)?;
            bail!("remote publication fenced: {reason}");
        }
        return Ok(());
    };
    if snapshot.remote_state.token.as_deref() == Some(current.token.as_str()) {
        return Ok(());
    }
    let head = match RemoteHead::parse(&current.bytes) {
        Ok(head) => head,
        Err(error) => {
            let reason = format!("changed remote head is invalid: {error:#}");
            fence_remote(db, &snapshot.remote_state, &reason)?;
            bail!("remote publication fenced: {reason}");
        }
    };
    if snapshot.remote_state.token.is_none()
        || head.database_id != snapshot.remote_state.database_id
        || head.owner != snapshot.remote_state.owner
        || head.lock.is_some()
        || head.sequence > snapshot.sequence
    {
        let reason = "changed remote head is not an owned unlocked prefix candidate";
        fence_remote(db, &snapshot.remote_state, reason)?;
        bail!("remote publication fenced: {reason}");
    }
    let segment_ids = match prove_owned_remote_prefix(&db.inner, &head, &snapshot) {
        Ok(segment_ids) => segment_ids,
        Err(error) => {
            let reason = format!("owned remote prefix proof failed: {error:#}");
            fence_remote(db, &snapshot.remote_state, &reason)?;
            bail!("remote publication fenced: {reason}");
        }
    };
    apply_binding(
        db,
        &snapshot.remote_state,
        PublishedHead {
            token: current.token,
            head,
            segment_ids,
        },
    )?;
    Ok(())
}

fn fence_remote(db: &Database, state: &RemoteState, reason: &str) -> Result<()> {
    let mut s = db.lock()?;
    if state.still_current(&s) {
        s.fenced = Some(format!(
            "remote head changed or binding is missing ({reason}); restore into a new directory rather than overwriting another publisher"
        ));
    }
    Ok(())
}

fn apply_binding(
    db: &Database,
    expected: &RemoteState,
    published: PublishedHead,
) -> Result<RemoteState> {
    let mut s = db.lock()?;
    if !expected.still_current(&s)
        || published.head.database_id != s.catalog.database_id
        || published.head.owner != s.remote_owner
        || published.head.sequence > s.sequence
    {
        s.fenced = Some(
            "remote publication succeeded but local binding state changed unexpectedly".into(),
        );
        bail!("remote publication fenced: local binding changed during remote I/O");
    }
    save_binding(&db.inner, &mut s, published.token, published.head)?;
    s.remote_segment_ids = published.segment_ids;
    Ok(RemoteState::capture(&s))
}

fn upload_ship(inner: &Inner, snapshot: &ShipSnapshot) -> Result<PublishedHead> {
    let remote = inner
        .remote
        .as_ref()
        .context("no remote store configured")?;
    match current_head_remote(inner, &snapshot.remote_state)? {
        CurrentHead::Match(Some(head)) => ensure!(
            head.lock.is_none(),
            "remote head is locked; resume GC or explicitly recover an abandoned restore lock"
        ),
        CurrentHead::Match(None) => {}
        CurrentHead::Mismatch(reason) => bail!("remote publication fenced: {reason}"),
    }
    let mut segment_ids = BTreeSet::new();
    for table in snapshot.checkpoint.tables.values() {
        for seg in &table.segments {
            let path = inner.root.join(seg.key());
            let bytes = if path.exists() {
                wal::read_bounded(&path, seg.bytes as usize)?
            } else {
                remote.get_bounded(&seg.key(), seg.bytes as usize)?
            };
            ensure!(
                bytes.len() as u64 == seg.bytes && blake3::hash(&bytes).to_hex().as_str() == seg.id,
                "checkpoint segment integrity failure"
            );
            remote.put_immutable(&seg.key(), &bytes)?;
            segment_ids.insert(seg.id.clone());
        }
    }
    let checkpoint_ref = ObjectRef::new(
        format!(
            "manifests/{}.bin",
            blake3::hash(&snapshot.checkpoint_bytes).to_hex()
        ),
        &snapshot.checkpoint_bytes,
    );
    remote.put_immutable(&checkpoint_ref.key, &snapshot.checkpoint_bytes)?;
    let mut tail = Vec::with_capacity(snapshot.wal.len());
    for record in &snapshot.wal {
        let hash = blake3::hash(&record.bytes).to_hex().to_string();
        let object = ObjectRef::new(
            format!("wal/{:020}-{hash}.wal", record.sequence),
            &record.bytes,
        );
        remote.put_immutable(&object.key, &record.bytes)?;
        tail.push(object);
    }
    wal::failpoint("remote_objects_uploaded");
    let head = RemoteHead {
        format_version: FORMAT_VERSION,
        database_id: snapshot.remote_state.database_id.clone(),
        owner: snapshot.remote_state.owner.clone(),
        sequence: snapshot.sequence,
        checkpoint: checkpoint_ref,
        wal: tail,
        lock: None,
    };
    let token = compare_and_swap_with_intent(inner, &snapshot.remote_state, &head)?;
    wal::failpoint("remote_head_published");
    Ok(PublishedHead {
        token,
        head,
        segment_ids,
    })
}

pub(crate) fn ship_with_remote_gate(
    db: &Database,
    _gate: &std::sync::MutexGuard<'_, ()>,
) -> Result<u64> {
    reconcile_owned_head(db)?;
    let snapshot = {
        let s = db.lock()?;
        healthy(&s)?;
        capture_ship(&db.inner, &s)?
    };
    let published = match upload_ship(&db.inner, &snapshot) {
        Ok(published) => published,
        Err(error) => {
            if error.to_string().starts_with("remote publication fenced:") {
                fence_remote(db, &snapshot.remote_state, &error.to_string())?;
            }
            return Err(error);
        }
    };
    apply_binding(db, &snapshot.remote_state, published)?;
    Ok(snapshot.sequence)
}

impl Database {
    pub fn ship(&self) -> Result<u64> {
        let gate = self.lock_remote_operation()?;
        ship_with_remote_gate(self, &gate)
    }

    pub fn vacuum_remote(&self) -> Result<usize> {
        let gate = self.lock_remote_operation()?;
        let resume = {
            let mut s = self.lock()?;
            healthy(&s)?;
            let resume = s.remote_head.as_ref().is_some_and(|head| {
                head.lock
                    .as_ref()
                    .is_some_and(|lock| lock.kind == "gc" && lock.owner == s.remote_owner)
            });
            if !resume {
                checkpoint_locked(&self.inner, &mut s)?;
            }
            resume
        };
        if !resume {
            ship_with_remote_gate(self, &gate)?;
        }
        let removed = vacuum_with_remote_gate(self, &gate)?;
        self.lock()?.remote_vacuum_pending = false;
        Ok(removed)
    }

    pub fn restore(
        path: impl AsRef<Path>,
        config: Config,
        remote: Arc<dyn RemoteStore>,
    ) -> Result<Self> {
        config.validate()?;
        let target = path.as_ref();
        ensure!(!target.exists(), "restore destination must not exist");
        if let Some(parent) = target.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }
        let absolute = if target.is_absolute() {
            target.to_path_buf()
        } else {
            std::env::current_dir()?.join(target)
        };
        let normalized = fs::canonicalize(absolute.parent().context("invalid restore parent")?)?
            .join(
                absolute
                    .file_name()
                    .context("invalid restore destination")?,
            );
        validate_remote_layout(&normalized, Some(remote.as_ref()))?;
        let current = remote
            .head()?
            .context("remote store has no committed head")?;
        let mut head = RemoteHead::parse(&current.bytes)?;
        ensure!(
            head.lock.is_none(),
            "remote head is locked; refusing concurrent restore/GC"
        );
        let operation_id = uuid::Uuid::new_v4().to_string();
        head.lock = Some(RemoteLock {
            kind: "restore".into(),
            owner: operation_id.clone(),
        });
        let locked_token =
            remote.compare_and_swap_head(Some(&current.token), &serde_json::to_vec(&head)?)?;
        let mut created = false;
        let result = (|| {
            fs::create_dir(target)?;
            created = true;
            let lock = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(target.join("LOCK"))?;
            lock.try_lock_exclusive()?;
            wal::atomic_write(&target.join("RESTORING"), operation_id.as_bytes())?;
            for directory in ["wal", "segments", "cache", "staging"] {
                fs::create_dir(target.join(directory))?;
            }
            let bytes = remote.get_bounded(&head.checkpoint.key, head.checkpoint.bytes as usize)?;
            head.checkpoint.verify(&bytes)?;
            let catalog = decode_manifest(&bytes)?;
            ensure!(
                catalog.database_id == head.database_id,
                "remote manifest identity mismatch"
            );
            ensure!(
                catalog.checkpoint_sequence <= head.sequence,
                "remote checkpoint is ahead of head"
            );
            ensure!(
                u64::try_from(head.wal.len()).ok().is_some_and(|tail_len| {
                    head.sequence.checked_sub(catalog.checkpoint_sequence) == Some(tail_len)
                }),
                "incomplete remote WAL tail"
            );
            let mut used = bytes.len() as u64;
            ensure!(
                used <= config.max_disk_bytes,
                "restore checkpoint exceeds disk budget"
            );
            wal::atomic_write(&target.join("manifest.bin"), &bytes)?;
            for (i, object) in head.wal.iter().enumerate() {
                let offset = u64::try_from(i)
                    .context("restore WAL index overflow")?
                    .checked_add(1)
                    .context("restore WAL index overflow")?;
                let sequence = catalog
                    .checkpoint_sequence
                    .checked_add(offset)
                    .context("restore sequence overflow")?;
                let bytes = remote.get_bounded(&object.key, object.bytes as usize)?;
                object.verify(&bytes)?;
                let record = wal::decode(&bytes)?;
                ensure!(
                    record.sequence == sequence
                        && object.key == format!("wal/{sequence:020}-{}.wal", object.digest),
                    "remote WAL sequence/key mismatch"
                );
                used = used
                    .checked_add(bytes.len() as u64)
                    .context("restore size overflow")?;
                ensure!(
                    used <= config.max_disk_bytes,
                    "restore WAL exceeds disk budget"
                );
                wal::atomic_write(&wal::path(target, sequence), &bytes)?;
            }
            // Segments remain remote and are fetched/verified on demand. Replay is validated before releasing the remote lease.
            let db = Self::open_locked(target, config.clone(), Some(remote.clone()), Some(lock))?;
            Ok(db)
        })();
        let mut released = head.clone();
        released.lock = None;
        if result.is_ok() {
            released.owner = operation_id;
        }
        let release =
            remote.compare_and_swap_head(Some(&locked_token), &serde_json::to_vec(&released)?);
        match (result, release) {
            (Ok(db), Ok(token)) => {
                {
                    let mut s = db.lock()?;
                    s.remote_owner = released.owner.clone();
                    s.remote_segment_ids = s
                        .catalog
                        .tables
                        .values()
                        .flat_map(|table| table.segments.iter().map(|segment| segment.id.clone()))
                        .collect();
                    save_binding(&db.inner, &mut s, token, released)?;
                }
                fs::remove_file(target.join("RESTORING"))?;
                wal::sync_dir(target)?;
                Ok(db)
            }
            (result, release) => {
                let reason = result
                    .as_ref()
                    .err()
                    .map(|e| format!("{e:#}"))
                    .unwrap_or_else(|| "lease release failed".into());
                drop(result);
                if created {
                    let _ = fs::remove_dir_all(target);
                }
                match release {
                    Err(error) => Err(error.context("restore failed or lease release failed; remote head remains locked/ambiguous; confirm old operation stopped before explicit lock recovery")),
                    Ok(_) => bail!("restore validation failed: {reason}; remote lease released and partial destination removed"),
                }
            }
        }
    }

    /// Administrative recovery only: the caller must ensure the lock owner is no longer running.
    /// Never use this as an automatic lease timeout or failover mechanism.
    pub fn recover_remote_lock(remote: Arc<dyn RemoteStore>, expected_owner: &str) -> Result<()> {
        let current = remote.head()?.context("remote head missing")?;
        let mut head = RemoteHead::parse(&current.bytes)?;
        let lock = head.lock.as_ref().context("remote head is not locked")?;
        ensure!(
            lock.owner == expected_owner,
            "lock owner changed; refusing to break lock"
        );
        head.lock = None;
        head.owner = uuid::Uuid::new_v4().to_string();
        remote.compare_and_swap_head(Some(&current.token), &serde_json::to_vec(&head)?)?;
        Ok(())
    }
}

pub(crate) fn vacuum_with_remote_gate(
    db: &Database,
    _gate: &std::sync::MutexGuard<'_, ()>,
) -> Result<usize> {
    let remote = db.inner.remote.as_ref().context("no remote configured")?;
    let mut state = {
        let s = db.lock()?;
        healthy(&s)?;
        RemoteState::capture(&s)
    };
    let mut head = match current_head_remote(&db.inner, &state)? {
        CurrentHead::Match(Some(head)) => head,
        CurrentHead::Match(None) => bail!("no published remote checkpoint"),
        CurrentHead::Mismatch(reason) => {
            fence_remote(db, &state, &reason)?;
            bail!("remote publication fenced: {reason}");
        }
    };
    let locked_token = if let Some(lock) = &head.lock {
        ensure!(
            lock.kind == "gc" && lock.owner == state.owner,
            "remote head locked by another operation"
        );
        state
            .token
            .clone()
            .context("local binding is missing the GC lock token")?
    } else {
        head.lock = Some(RemoteLock {
            kind: "gc".into(),
            owner: state.owner.clone(),
        });
        let token = compare_and_swap_with_intent(&db.inner, &state, &head)?;
        wal::failpoint("remote_gc_lock_acquired");
        wal::failpoint("remote_head_published");
        state = apply_binding(
            db,
            &state,
            PublishedHead {
                token: token.clone(),
                head: head.clone(),
                segment_ids: state.segment_ids.clone(),
            },
        )?;
        token
    };
    let mut published_ids = None;
    let result = (|| {
        let bytes = remote.get_bounded(&head.checkpoint.key, head.checkpoint.bytes as usize)?;
        head.checkpoint.verify(&bytes)?;
        let checkpoint = decode_manifest(&bytes)?;
        ensure!(
            checkpoint.database_id == head.database_id
                && checkpoint.checkpoint_sequence <= head.sequence,
            "remote GC checkpoint does not match the committed head"
        );
        let ids: BTreeSet<_> = checkpoint
            .tables
            .values()
            .flat_map(|table| table.segments.iter().map(|segment| segment.id.clone()))
            .collect();
        let mut live: BTreeSet<_> = checkpoint
            .tables
            .values()
            .flat_map(|table| table.segments.iter().map(Segment::key))
            .collect();
        live.insert(head.checkpoint.key.clone());
        live.extend(head.wal.iter().map(|record| record.key.clone()));
        let mut removed = 0usize;
        for prefix in ["segments", "manifests", "wal"] {
            let mut after = None;
            let mut cursors = BTreeSet::new();
            let mut pages = 0usize;
            loop {
                pages = pages
                    .checked_add(1)
                    .context("remote vacuum page count overflow")?;
                ensure!(
                    pages <= 10_000,
                    "remote vacuum exceeds bounded per-prefix page limit"
                );
                let page = remote.list_page(prefix, after.as_deref(), MAX_LIST_PAGE_SIZE)?;
                let mut unreachable = Vec::new();
                for key in page.keys {
                    ensure!(
                        key.starts_with(&format!("{prefix}/")),
                        "remote listing escaped requested prefix"
                    );
                    if !live.contains(&key) {
                        unreachable.push(key);
                    }
                }
                if !unreachable.is_empty() {
                    removed = removed
                        .checked_add(remote.delete_batch(&unreachable)?)
                        .context("remote vacuum deletion count overflow")?;
                }
                let Some(next) = page.next else { break };
                ensure!(cursors.insert(next.clone()), "remote listing cursor cycle");
                after = Some(next);
            }
        }
        published_ids = Some(ids);
        Ok(removed)
    })();
    // Only unreachable objects were deleted; a partially completed vacuum is safe to retry.
    // Preserve the lock if release fails. No time-based stealing is attempted.
    let mut released = head;
    released.lock = None;
    ensure!(
        state.token.as_deref() == Some(locked_token.as_str()),
        "local binding lost the GC lock token"
    );
    let token = compare_and_swap_with_intent(&db.inner, &state, &released)?;
    wal::failpoint("remote_gc_lock_released");
    wal::failpoint("remote_head_published");
    apply_binding(
        db,
        &state,
        PublishedHead {
            token,
            head: released,
            segment_ids: published_ids.unwrap_or_else(|| state.segment_ids.clone()),
        },
    )?;
    result
}
