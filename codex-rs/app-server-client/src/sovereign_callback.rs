//! Explicitly admitted local owner for the Sovereign `witnessops/read_head` callback.
//!
//! This adapter is opt-in: constructing a general app-server client never starts it.
//! Its operator supplies the repository, allowed threads and verified server generation.
//! Callers retain this object across transport reconnection, feed it notifications, and
//! poll until cleanup and delivery settle. A new process must not resume an uncertain
//! action; this in-memory outbox is not durable crash recovery. Capabilities are private.
//! Linux directory descriptors pin repository identity through child reaping. They
//! prevent path substitution; repository contents remain a read of current local HEAD.

use crate::AppServerRequestHandle;
pub use crate::sovereign_supervisor::SovereignCallbackSupervisor;
use codex_app_server::DYNAMIC_TOOL_RECOVERY_RECORD_LIMIT;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::DynamicToolCallOutputContentItem;
use codex_app_server_protocol::DynamicToolCallResponse;
use codex_app_server_protocol::DynamicToolCallStatus;
use codex_app_server_protocol::DynamicToolCleanupAckParams;
use codex_app_server_protocol::DynamicToolCleanupAckResponse;
use codex_app_server_protocol::DynamicToolCleanupIdentity;
use codex_app_server_protocol::DynamicToolCleanupReadParams;
use codex_app_server_protocol::DynamicToolCleanupReadResponse;
use codex_app_server_protocol::DynamicToolCleanupStatus;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;
use codex_app_server_protocol::ThreadItemPayload;
use std::collections::HashSet;
use std::fmt;
use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::watch;
use tokio::task::JoinHandle;

const MAX_THREADS: usize = 64;
const MAX_ACTIVE: usize = 64;
const MAX_ID_BYTES: usize = 256;
const RPC_TIMEOUT: Duration = Duration::from_secs(2);
const EXECUTION_TIMEOUT: Duration = Duration::from_secs(5);
const CLEANUP_REPORT_TIMEOUT: Duration = Duration::from_secs(2);
static NEXT_REQUEST: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallbackState {
    Running,
    CleanupPending,
    AckPending,
    Acknowledged,
    Hold,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallbackHoldReason {
    TransportUnavailable,
    ServerIdentityUnconfirmed,
    CleanupUnconfirmed,
}

/// Safe to display: neither capabilities nor untrusted transport diagnostics appear here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallbackProgress {
    pub request_id: RequestId,
    pub thread_id: String,
    pub turn_id: String,
    pub call_id: String,
    pub state: CallbackState,
    pub hold_reason: Option<CallbackHoldReason>,
    pub process_id: Option<u32>,
    pub reaped: bool,
    pub execution_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SovereignCallbackError {
    NotAdmitted,
    MissingCapability,
    Replay,
    WrongGeneration,
    Disconnected,
    Capacity,
    ServerStateUnconfirmed,
}

impl fmt::Display for SovereignCallbackError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotAdmitted => "callback is outside the admitted read_head scope",
            Self::MissingCapability => "callback has no valid cleanup capability",
            Self::Replay => "callback identity was already consumed",
            Self::WrongGeneration => "callback belongs to a different server generation",
            Self::Disconnected => "callback owner is disconnected",
            Self::Capacity => "callback owner retention limit reached",
            Self::ServerStateUnconfirmed => "callback server state is not confirmed running",
        })
    }
}
impl std::error::Error for SovereignCallbackError {}

#[derive(Clone)]
struct AdmittedRoot {
    directory: Arc<File>,
    git: Arc<File>,
}

#[cfg(test)]
#[derive(Clone)]
struct SpawnGate {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[derive(Clone, Copy, Default)]
struct Observation {
    process_id: Option<u32>,
    reaped: bool,
    uncertain: bool,
}

struct Call {
    identity: DynamicToolCleanupIdentity,
    cancel: watch::Sender<bool>,
    observation: watch::Receiver<Observation>,
    worker: Option<JoinHandle<Option<DynamicToolCallResponse>>>,
    outbox: Option<DynamicToolCallResponse>,
    state: CallbackState,
    hold_reason: Option<CallbackHoldReason>,
    cancellation_requested: bool,
    execution_count: usize,
}

impl Call {
    fn cancel(&mut self) {
        if self.worker.is_some() {
            self.cancellation_requested = true;
            self.state = CallbackState::CleanupPending;
            let _ = self.cancel.send(true);
        }
    }

    fn progress(&self) -> CallbackProgress {
        let observed = *self.observation.borrow();
        CallbackProgress {
            request_id: self.identity.request_id.clone(),
            thread_id: self.identity.thread_id.clone(),
            turn_id: self.identity.turn_id.clone(),
            call_id: self.identity.call_id.clone(),
            state: if observed.uncertain {
                CallbackState::Hold
            } else {
                self.state
            },
            hold_reason: if observed.uncertain {
                Some(CallbackHoldReason::CleanupUnconfirmed)
            } else {
                self.hold_reason
            },
            process_id: observed.process_id,
            reaped: observed.reaped,
            execution_count: self.execution_count,
        }
    }
}

/// Owns admitted execution and an acknowledgement-only retry outbox.
///
/// No task is restarted on disconnect, failed delivery, duplicate requests or a
/// changed server instance. Completed identities remain retained for this owner's
/// lifetime. `Drop` requests cooperative cancellation, but is never cleanup proof;
/// callers should keep polling the retained owner until its state is settled.
pub struct SovereignCallbackClient {
    handle: AppServerRequestHandle,
    generation: String,
    root: AdmittedRoot,
    allowed_threads: HashSet<String>,
    calls: Vec<Call>,
    connected: bool,
    next_poll: usize,
    execution_guard: Option<Arc<std::sync::Mutex<bool>>>,
    #[cfg(test)]
    completion_gate: Option<std::sync::Arc<tokio::sync::Notify>>,
    #[cfg(test)]
    spawn_gate: Option<SpawnGate>,
}

impl SovereignCallbackClient {
    pub(crate) fn set_execution_guard(&mut self, guard: Arc<std::sync::Mutex<bool>>) {
        self.execution_guard = Some(guard);
    }

    pub(crate) fn request_handle(&self) -> AppServerRequestHandle {
        self.handle.clone()
    }

    pub(crate) fn new_unassigned(
        handle: AppServerRequestHandle,
        generation: String,
        root: PathBuf,
    ) -> io::Result<Self> {
        let mut owner = Self::new(handle, generation, root, ["unassigned-owner".into()])?;
        owner.allowed_threads.clear();
        Ok(owner)
    }

    pub(crate) fn admit_thread(&mut self, thread: String) -> Result<(), SovereignCallbackError> {
        if !valid_id(&thread)
            || (!self.allowed_threads.contains(&thread)
                && self.allowed_threads.len() >= MAX_THREADS)
        {
            return Err(SovereignCallbackError::NotAdmitted);
        }
        self.allowed_threads.insert(thread);
        Ok(())
    }

    /// Cancel cooperatively while preserving the channel needed to acknowledge.
    pub fn request_cleanup(&mut self) {
        if let Some(guard) = &self.execution_guard
            && let Ok(mut permitted) = guard.lock()
        {
            *permitted = false;
        }
        for call in &mut self.calls {
            call.cancel();
        }
    }

    pub(crate) async fn reconnect_verified(
        &mut self,
        handle: AppServerRequestHandle,
    ) -> Result<(), SovereignCallbackError> {
        self.disconnected();
        // A handshake flag or endpoint name does not establish server generation.
        // At least one retained unforgeable identity must still be recognized.
        if self.calls.is_empty() {
            return Err(SovereignCallbackError::ServerStateUnconfirmed);
        }
        // One retained capability proves continuity with this server instance;
        // every individual outbox still validates its own identity before ack.
        let call = &self.calls[0];
        if read_status(&handle, call.identity.clone()).await.is_none() {
            return Err(SovereignCallbackError::ServerStateUnconfirmed);
        }
        self.handle = handle;
        self.connected = true;
        Ok(())
    }
    pub fn new(
        handle: AppServerRequestHandle,
        server_generation: String,
        root: PathBuf,
        allowed_threads: impl IntoIterator<Item = String>,
    ) -> io::Result<Self> {
        if !cfg!(target_os = "linux") || !valid_id(&server_generation) || !root.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid callback owner scope",
            ));
        }
        let allowed_threads: HashSet<_> = allowed_threads.into_iter().collect();
        if allowed_threads.is_empty()
            || allowed_threads.len() > MAX_THREADS
            || allowed_threads.iter().any(|thread| !valid_id(thread))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid callback thread admission",
            ));
        }
        let root = pin_repository(root)?;
        Ok(Self {
            handle,
            generation: server_generation,
            root,
            allowed_threads,
            calls: Vec::new(),
            connected: true,
            next_poll: 0,
            execution_guard: None,
            #[cfg(test)]
            completion_gate: None,
            #[cfg(test)]
            spawn_gate: None,
        })
    }

    /// Returns false for another namespace; rejects every nonempty argument or
    /// operation other than the single fixed read. Identity is consumed before I/O.
    pub async fn dispatch(
        &mut self,
        generation: &str,
        request: &ServerRequest,
    ) -> Result<bool, SovereignCallbackError> {
        self.dispatch_inner(generation, request, true).await
    }

    pub(crate) async fn decline_execution(
        &mut self,
        generation: &str,
        request: &ServerRequest,
    ) -> Result<bool, SovereignCallbackError> {
        self.dispatch_inner(generation, request, false).await
    }

    async fn dispatch_inner(
        &mut self,
        generation: &str,
        request: &ServerRequest,
        execute: bool,
    ) -> Result<bool, SovereignCallbackError> {
        self.check_generation(generation)?;
        let ServerRequest::DynamicToolCall { request_id, params } = request else {
            return Ok(false);
        };
        if params.namespace.as_deref() != Some("witnessops") {
            return Ok(false);
        }
        if params.tool != "read_head"
            || !params
                .arguments
                .as_object()
                .is_some_and(serde_json::Map::is_empty)
            || !self.allowed_threads.contains(&params.thread_id)
            || !valid_id(&params.turn_id)
            || !valid_id(&params.call_id)
            || !valid_request_id(request_id)
        {
            return Err(SovereignCallbackError::NotAdmitted);
        }
        let capability = params
            .cleanup_token
            .as_ref()
            .filter(|token| !token.is_empty() && token.len() <= 512)
            .ok_or(SovereignCallbackError::MissingCapability)?;
        if self.calls.iter().any(|call| {
            call.identity.request_id == *request_id
                || (call.identity.thread_id == params.thread_id
                    && call.identity.turn_id == params.turn_id
                    && call.identity.call_id == params.call_id)
                || call.identity.capability == *capability
        }) {
            return Err(SovereignCallbackError::Replay);
        }
        // The server refuses issuance above this same lifetime retention bound.
        // Execution concurrency is a separate, smaller budget: already-issued
        // callbacks above it must still retain a cleanup-only outcome.
        if self.calls.len() == DYNAMIC_TOOL_RECOVERY_RECORD_LIMIT {
            return Err(SovereignCallbackError::Capacity);
        }
        let at_active_limit = self
            .calls
            .iter()
            .filter(|call| call.worker.is_some())
            .count()
            >= MAX_ACTIVE;
        let identity = DynamicToolCleanupIdentity {
            thread_id: params.thread_id.clone(),
            turn_id: params.turn_id.clone(),
            call_id: params.call_id.clone(),
            request_id: request_id.clone(),
            capability: capability.clone(),
        };
        let (cancel, cancel_rx) = watch::channel(false);
        let (observation_tx, observation) = watch::channel(Observation::default());
        self.calls.push(Call {
            identity: identity.clone(),
            cancel,
            observation,
            worker: None,
            outbox: None,
            state: CallbackState::Hold,
            hold_reason: Some(CallbackHoldReason::ServerIdentityUnconfirmed),
            cancellation_requested: false,
            execution_count: 0,
        });
        if !self.connected {
            let call = self.calls.last_mut().expect("identity retained");
            call.outbox = Some(cleanup_response(
                false,
                "read_head disconnected before execution",
            ));
            return Err(SovereignCallbackError::Disconnected);
        }
        let status = read_status(&self.handle, identity).await;
        let execution_permitted = self
            .execution_guard
            .as_ref()
            .is_none_or(|guard| guard.lock().is_ok_and(|permitted| *permitted));
        let call = self.calls.last_mut().expect("identity retained before I/O");
        match status {
            Some(DynamicToolCleanupStatus::Acknowledged) => {
                call.state = CallbackState::Acknowledged;
                call.hold_reason = None;
                return Ok(true);
            }
            Some(DynamicToolCleanupStatus::Cancelling) => {
                call.outbox = Some(cleanup_response(
                    false,
                    "read_head cancelled before execution",
                ));
                call.state = CallbackState::AckPending;
                call.hold_reason = None;
                return Ok(true);
            }
            Some(DynamicToolCleanupStatus::OwnerDetached) => {
                // An older executor may still own work. Do not claim its cleanup.
                return Err(SovereignCallbackError::ServerStateUnconfirmed);
            }
            None => {
                // Consume identity once, then permit only identity-checked cleanup
                // delivery. An uncertain admission can never start work later.
                call.outbox = Some(cleanup_response(
                    false,
                    "read_head admission remained unconfirmed; no operation was started",
                ));
                return Err(SovereignCallbackError::ServerStateUnconfirmed);
            }
            Some(DynamicToolCleanupStatus::Running)
                if at_active_limit || !execute || !execution_permitted =>
            {
                call.outbox = Some(cleanup_response(
                    false,
                    "read_head execution not admitted; no operation was started",
                ));
                call.state = CallbackState::AckPending;
                call.hold_reason = None;
                return Ok(true);
            }
            Some(DynamicToolCleanupStatus::Running) => {}
        }
        let root = self.root.clone();
        let execution_guard = self.execution_guard.clone();
        #[cfg(test)]
        let gate = self.completion_gate.clone();
        #[cfg(test)]
        let spawn_gate = self.spawn_gate.clone();
        let worker = tokio::spawn(async move {
            execute_read_head(
                root,
                cancel_rx,
                observation_tx,
                execution_guard,
                #[cfg(test)]
                gate,
                #[cfg(test)]
                spawn_gate,
            )
            .await
        });
        if let Some(call) = self.calls.last_mut() {
            call.worker = Some(worker);
            call.execution_count = 1;
            call.state = CallbackState::Running;
            call.hold_reason = None;
        }
        Ok(true)
    }

    pub fn observe_notification(
        &mut self,
        generation: &str,
        notification: &ServerNotification,
    ) -> Result<(), SovereignCallbackError> {
        self.check_generation(generation)?;
        if let ServerNotification::ItemCompleted(item) = notification
            && let ThreadItemPayload::DynamicToolCall {
                id,
                status: DynamicToolCallStatus::Failed,
                ..
            } = &item.item.payload
        {
            for call in &mut self.calls {
                if call.identity.thread_id == item.thread_id
                    && call.identity.turn_id == item.turn_id
                    && call.identity.call_id == *id
                {
                    call.cancel();
                }
            }
        }
        Ok(())
    }

    pub fn disconnected(&mut self) {
        self.connected = false;
        for call in &mut self.calls {
            call.cancel();
        }
    }

    pub fn reconnect(
        &mut self,
        generation: &str,
        handle: AppServerRequestHandle,
    ) -> Result<(), SovereignCallbackError> {
        self.check_generation(generation)?;
        self.handle = handle;
        self.connected = true;
        Ok(())
    }

    /// Polling never runs an operation again. Only a finished, reaped worker can
    /// produce an outbox entry; transport errors retain that immutable response.
    /// At most one call performs status/ack I/O per poll, so a stalled peer cannot
    /// multiply the RPC deadline by the retention limit. Pending calls rotate.
    pub async fn poll(&mut self) -> Vec<CallbackProgress> {
        for call in &mut self.calls {
            if call.worker.as_ref().is_some_and(JoinHandle::is_finished) {
                let worker = call.worker.take().expect("finished worker exists");
                match worker.await {
                    Ok(Some(mut response)) => {
                        if call.cancellation_requested {
                            response = cleanup_response(false, "read_head cancelled after cleanup");
                        }
                        call.outbox = Some(response);
                        call.state = CallbackState::AckPending;
                        call.hold_reason = None;
                    }
                    _ => {
                        call.state = CallbackState::Hold;
                        call.hold_reason = Some(CallbackHoldReason::CleanupUnconfirmed);
                    }
                }
            }
            if !self.connected && call.outbox.is_some() {
                call.state = CallbackState::AckPending;
                call.hold_reason = Some(CallbackHoldReason::TransportUnavailable);
            }
        }
        if !self.connected || self.calls.is_empty() {
            return self.snapshots();
        }
        let call_count = self.calls.len();
        for offset in 0..call_count {
            let index = (self.next_poll + offset) % call_count;
            let call = &mut self.calls[index];
            if call.state == CallbackState::Acknowledged || call.outbox.is_none() {
                continue;
            }
            self.next_poll = (index + 1) % call_count;
            match read_status(&self.handle, call.identity.clone()).await {
                Some(DynamicToolCleanupStatus::Acknowledged) => {
                    call.outbox = None;
                    call.state = CallbackState::Acknowledged;
                    call.hold_reason = None;
                }
                Some(_) => {
                    let response = call.outbox.as_ref().expect("outbox checked").clone();
                    let request = ClientRequest::DynamicToolCleanupAck {
                        request_id: next_request_id(),
                        params: DynamicToolCleanupAckParams {
                            identity: call.identity.clone(),
                            response,
                        },
                    };
                    match tokio::time::timeout(
                        RPC_TIMEOUT,
                        self.handle
                            .request_typed::<DynamicToolCleanupAckResponse>(request),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            call.outbox = None;
                            call.state = CallbackState::Acknowledged;
                            call.hold_reason = None;
                        }
                        _ => {
                            call.state = CallbackState::AckPending;
                            call.hold_reason = Some(CallbackHoldReason::TransportUnavailable);
                        }
                    }
                }
                None => {
                    call.state = CallbackState::Hold;
                    call.hold_reason = Some(CallbackHoldReason::ServerIdentityUnconfirmed);
                }
            }
            break;
        }
        self.snapshots()
    }

    pub fn snapshots(&self) -> Vec<CallbackProgress> {
        self.calls.iter().map(Call::progress).collect()
    }

    fn check_generation(&self, generation: &str) -> Result<(), SovereignCallbackError> {
        if generation == self.generation {
            Ok(())
        } else {
            Err(SovereignCallbackError::WrongGeneration)
        }
    }

    #[cfg(test)]
    pub(crate) fn set_test_completion_gate(&mut self, gate: std::sync::Arc<tokio::sync::Notify>) {
        self.completion_gate = Some(gate);
    }
}

impl Drop for SovereignCallbackClient {
    fn drop(&mut self) {
        // Dropping the JoinHandle detaches the owner; it does not abort its wait/reap.
        // No acknowledgement is manufactured when the caller abandons this object.
        for call in &mut self.calls {
            call.cancel();
        }
    }
}

fn valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_ID_BYTES && !value.chars().any(char::is_control)
}
fn valid_request_id(value: &RequestId) -> bool {
    match value {
        RequestId::String(value) => valid_id(value),
        RequestId::Integer(value) => *value >= 0,
    }
}
fn next_request_id() -> RequestId {
    RequestId::String(format!(
        "sovereign-cleanup-{}-{}",
        std::process::id(),
        NEXT_REQUEST.fetch_add(1, Ordering::Relaxed)
    ))
}
async fn read_status(
    handle: &AppServerRequestHandle,
    identity: DynamicToolCleanupIdentity,
) -> Option<DynamicToolCleanupStatus> {
    let request = ClientRequest::DynamicToolCleanupRead {
        request_id: next_request_id(),
        params: DynamicToolCleanupReadParams { identity },
    };
    // Do not retain/display errors: arbitrary peer diagnostics can contain secrets.
    match tokio::time::timeout(
        RPC_TIMEOUT,
        handle.request_typed::<DynamicToolCleanupReadResponse>(request),
    )
    .await
    {
        Ok(Ok(response)) => Some(response.status),
        _ => None,
    }
}
fn cleanup_response(success: bool, text: impl Into<String>) -> DynamicToolCallResponse {
    DynamicToolCallResponse {
        content_items: vec![DynamicToolCallOutputContentItem::InputText { text: text.into() }],
        success,
        cleanup_complete: true,
    }
}
#[cfg(target_os = "linux")]
fn pin_repository(path: PathBuf) -> io::Result<AdmittedRoot> {
    use std::os::fd::AsRawFd;
    let directory = Arc::new(pin_directory(&path)?);
    // Resolve .git relative to the already-open directory. O_NOFOLLOW rejects
    // linked-worktree files and a substituted final symlink during admission.
    let git = Arc::new(pin_directory(&PathBuf::from(format!(
        "/proc/self/fd/{}/.git",
        directory.as_raw_fd()
    )))?);
    Ok(AdmittedRoot { directory, git })
}

#[cfg(target_os = "linux")]
fn pin_directory(path: &std::path::Path) -> io::Result<File> {
    use std::os::fd::AsRawFd;
    use std::os::fd::FromRawFd;
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    // Keep pinned descriptors above stdio: child stdio setup must not replace
    // one if the embedding process happened to start with fd 0, 1 or 2 closed.
    // SAFETY: fcntl borrows a live descriptor and returns a new owned descriptor.
    let pinned = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
    if pinned < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: pinned is the successful, uniquely-owned duplicate from fcntl.
    Ok(unsafe { File::from_raw_fd(pinned) })
}

#[cfg(not(target_os = "linux"))]
fn pin_repository(_path: PathBuf) -> io::Result<AdmittedRoot> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "read_head requires Linux",
    ))
}

#[cfg(target_os = "linux")]
fn configure_pinned_repository(command: &mut Command, root: &AdmittedRoot) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let directory_fd = root.directory.as_raw_fd();
    let git_fd = root.git.as_raw_fd();
    command
        .arg("--git-dir")
        .arg(format!("/proc/self/fd/{git_fd}"))
        .args(["--work-tree", "."]);
    // The root and .git File handles remain owned by execute_read_head through
    // reaping. In the child only, enter the pinned root and preserve the pinned
    // git descriptor across exec. The parent descriptors always remain CLOEXEC.
    // SAFETY: the closure only invokes async-signal-safe fchdir/fcntl and reads
    // errno; both borrowed descriptors remain live while spawn invokes it.
    unsafe {
        command.pre_exec(move || {
            if libc::fchdir(directory_fd) < 0 {
                return Err(io::Error::last_os_error());
            }
            let flags = libc::fcntl(git_fd, libc::F_GETFD);
            if flags < 0 || libc::fcntl(git_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn configure_pinned_repository(_command: &mut Command, _root: &AdmittedRoot) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "read_head requires Linux",
    ))
}

async fn execute_read_head(
    root: AdmittedRoot,
    mut cancel: watch::Receiver<bool>,
    observation: watch::Sender<Observation>,
    execution_guard: Option<Arc<std::sync::Mutex<bool>>>,
    #[cfg(test)] completion_gate: Option<std::sync::Arc<tokio::sync::Notify>>,
    #[cfg(test)] spawn_gate: Option<SpawnGate>,
) -> Option<DynamicToolCallResponse> {
    if *cancel.borrow() {
        return Some(cleanup_response(
            false,
            "read_head cancelled before execution",
        ));
    }
    #[cfg(test)]
    if let Some(gate) = spawn_gate {
        gate.entered.notify_one();
        gate.release.notified().await;
    }
    if *cancel.borrow() {
        return Some(cleanup_response(
            false,
            "read_head cancelled before execution",
        ));
    }
    let mut command = Command::new("/usr/bin/git");
    command.args([
        "--no-pager",
        "--no-optional-locks",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.fsmonitor=false",
    ]);
    if configure_pinned_repository(&mut command, &root).is_err() {
        return Some(cleanup_response(
            false,
            "read_head repository pin unavailable",
        ));
    }
    command
        .args(["rev-parse", "--verify", "HEAD"])
        .env_clear()
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("LANG", "C")
        .env("LC_ALL", "C")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let child = {
        // Revocation and synchronous process creation share one lock. Once
        // disconnect/drain observes revocation, no deferred worker may spawn.
        let permit = execution_guard.as_ref().map(|guard| guard.lock());
        if permit
            .as_ref()
            .is_some_and(|permit| !permit.as_ref().is_ok_and(|permitted| **permitted))
        {
            return Some(cleanup_response(
                false,
                "read_head execution admission revoked",
            ));
        }
        command.spawn()
    };
    let Ok(mut child) = child else {
        return Some(cleanup_response(false, "read_head could not start"));
    };
    let mut observed = Observation {
        process_id: child.id(),
        ..Observation::default()
    };
    let _ = observation.send(observed);
    let stdout = child.stdout.take();
    let (mut status, cancelled) = tokio::select! {
        biased;
        _ = async { if !*cancel.borrow() { let _ = cancel.changed().await; } } => {
            let _ = child.start_kill();
            (wait_for_reap(&mut child).await, true)
        }
        status = child.wait() => (status, false),
        _ = tokio::time::sleep(EXECUTION_TIMEOUT) => {
            let _ = child.start_kill();
            (wait_for_reap(&mut child).await, true)
        }
    };
    // Retain the actual child owner on an uncertain wait. A later poll can see
    // HOLD, while only another successful wait may establish reaping.
    while status.is_err() {
        observed.uncertain = true;
        let _ = observation.send(observed);
        let _ = child.start_kill();
        tokio::time::sleep(Duration::from_secs(1)).await;
        status = wait_for_reap(&mut child).await;
    }
    observed.reaped = true;
    observed.uncertain = false;
    let _ = observation.send(observed);
    let mut output = Vec::new();
    let output_read = async {
        let Some(stdout) = stdout else {
            return Err(io::Error::other("missing stdout"));
        };
        stdout.take(257).read_to_end(&mut output).await
    };
    if !matches!(
        tokio::time::timeout(Duration::from_secs(1), output_read).await,
        Ok(Ok(_))
    ) {
        return None;
    }
    #[cfg(test)]
    if let Some(gate) = completion_gate {
        gate.notified().await;
    }
    if cancelled || *cancel.borrow() {
        return Some(cleanup_response(false, "read_head cancelled after cleanup"));
    }
    let successful = status.is_ok_and(|status| status.success());
    let head = std::str::from_utf8(&output)
        .ok()
        .and_then(|value| value.strip_suffix('\n'));
    match head.filter(|head| {
        successful
            && matches!(head.len(), 40 | 64)
            && head
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }) {
        Some(head) => Some(cleanup_response(
            true,
            serde_json::json!({"head": head}).to_string(),
        )),
        None => Some(cleanup_response(
            false,
            "read_head did not return a bounded commit identity",
        )),
    }
}

// An expired reporting deadline leaves the Child owned by the worker. It only
// changes visible progress to HOLD; it never claims termination or drops the wait.
async fn wait_for_reap(child: &mut tokio::process::Child) -> io::Result<std::process::ExitStatus> {
    tokio::time::timeout(CLEANUP_REPORT_TIMEOUT, child.wait())
        .await
        .unwrap_or_else(|_| {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "child cleanup remains unconfirmed",
            ))
        })
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::ClientCommand;
    use crate::InProcessAppServerRequestHandle;
    use codex_app_server_protocol::DynamicToolCallParams;
    use pretty_assertions::assert_eq;
    use tokio::sync::mpsc;

    async fn repository() -> tempfile::TempDir {
        let root = tempfile::tempdir().expect("fixture root");
        for arguments in [
            vec!["init", "--quiet"],
            vec![
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "-c",
                "core.hooksPath=/dev/null",
                "commit",
                "--quiet",
                "--allow-empty",
                "-m",
                "fixture",
            ],
        ] {
            let result = Command::new("/usr/bin/git")
                .args(arguments)
                .current_dir(root.path())
                .env_clear()
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("LANG", "C")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await
                .expect("fixture git invocation");
            assert!(result.success());
        }
        root
    }

    fn handle() -> (AppServerRequestHandle, mpsc::Receiver<ClientCommand>) {
        let (command_tx, command_rx) = mpsc::channel(8);
        (
            AppServerRequestHandle::InProcess(InProcessAppServerRequestHandle { command_tx }),
            command_rx,
        )
    }

    fn owner(handle: AppServerRequestHandle, root: &tempfile::TempDir) -> SovereignCallbackClient {
        SovereignCallbackClient::new(
            handle,
            "server-generation".into(),
            root.path().to_path_buf(),
            ["thread".into()],
        )
        .expect("admitted owner")
    }

    fn callback() -> ServerRequest {
        ServerRequest::DynamicToolCall {
            request_id: RequestId::Integer(1),
            params: DynamicToolCallParams {
                cleanup_token: Some("fixture-private-capability".into()),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                call_id: "call".into(),
                namespace: Some("witnessops".into()),
                tool: "read_head".into(),
                arguments: serde_json::json!({}),
            },
        }
    }

    #[tokio::test]
    async fn real_read_head_returns_only_commit_identity_after_child_reaped() {
        let root = repository().await;
        let (handle, _commands) = handle();
        let owner = owner(handle, &root);
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let (observation_tx, observation_rx) = watch::channel(Observation::default());
        let response = execute_read_head(
            owner.root.clone(),
            cancel_rx,
            observation_tx,
            None,
            None,
            None,
        )
        .await
        .expect("cleanup confirmed");
        assert!(response.success && response.cleanup_complete);
        let [DynamicToolCallOutputContentItem::InputText { text }] =
            response.content_items.as_slice()
        else {
            panic!("single text result");
        };
        let result: serde_json::Value = serde_json::from_str(text).expect("bounded head JSON");
        assert_eq!(result.as_object().expect("object").len(), 1);
        assert!(matches!(
            result["head"].as_str().expect("head").len(),
            40 | 64
        ));
        let observed = *observation_rx.borrow();
        assert!(observed.reaped && !observed.uncertain);
        assert!(
            !PathBuf::from(format!(
                "/proc/{}",
                observed.process_id.expect("actual child pid")
            ))
            .exists()
        );
    }

    #[tokio::test]
    async fn cancellation_kills_and_reaps_actual_blocked_git_before_confirming_cleanup() {
        let root = repository().await;
        // A fixture FIFO makes the fixed git command block in a real read. No
        // arbitrary executable or alternate production command is admitted.
        std::fs::remove_file(root.path().join(".git/HEAD")).expect("replace fixture HEAD");
        assert!(
            Command::new("/usr/bin/mkfifo")
                .arg(root.path().join(".git/HEAD"))
                .env_clear()
                .status()
                .await
                .expect("fixture FIFO")
                .success()
        );
        let (handle, _commands) = handle();
        let owner = owner(handle, &root);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (observation_tx, mut observation_rx) = watch::channel(Observation::default());
        let worker = tokio::spawn(execute_read_head(
            owner.root.clone(),
            cancel_rx,
            observation_tx,
            None,
            None,
            None,
        ));
        tokio::time::timeout(
            Duration::from_secs(2),
            observation_rx.wait_for(|observed| observed.process_id.is_some()),
        )
        .await
        .expect("spawn observed")
        .expect("owner alive");
        let pid = observation_rx.borrow().process_id.expect("child pid");
        assert!(!observation_rx.borrow().reaped);
        assert!(PathBuf::from(format!("/proc/{pid}")).exists());
        cancel_tx
            .send(true)
            .expect("request cooperative cancellation");
        let response = tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .expect("bounded cleanup")
            .expect("worker joined")
            .expect("cleanup confirmed");
        assert!(!response.success && response.cleanup_complete);
        assert!(observation_rx.borrow().reaped);
        assert!(!PathBuf::from(format!("/proc/{pid}")).exists());
    }

    #[tokio::test]
    async fn admission_rejects_foreign_threads_arguments_and_missing_capability_without_io() {
        let root = repository().await;
        let (handle, mut commands) = handle();
        let mut owner = owner(handle, &root);
        for variant in ["thread", "arguments", "capability", "operation"] {
            let mut request = callback();
            let ServerRequest::DynamicToolCall { params, .. } = &mut request else {
                unreachable!()
            };
            match variant {
                "thread" => params.thread_id = "unadmitted".into(),
                "arguments" => params.arguments = serde_json::json!({"path":"/etc/passwd"}),
                "capability" => params.cleanup_token = None,
                _ => params.tool = "write_head".into(),
            }
            assert!(owner.dispatch("server-generation", &request).await.is_err());
        }
        assert!(owner.snapshots().is_empty());
        assert!(commands.try_recv().is_err());
        assert_eq!(
            owner.dispatch("other-generation", &callback()).await,
            Err(SovereignCallbackError::WrongGeneration)
        );
    }

    #[tokio::test]
    async fn unconfirmed_server_state_consumes_identity_without_execution_or_secret_diagnostics() {
        let root = repository().await;
        let (handle, mut commands) = handle();
        let mut owner = owner(handle, &root);
        let responder = tokio::spawn(async move {
            let Some(ClientCommand::Request { response_tx, .. }) = commands.recv().await else {
                panic!("status read");
            };
            let _ = response_tx.send(Ok(Ok(
                serde_json::json!({"status":"fixture-private-capability"}),
            )));
        });
        assert_eq!(
            owner.dispatch("server-generation", &callback()).await,
            Err(SovereignCallbackError::ServerStateUnconfirmed)
        );
        responder.await.expect("status response");
        assert_eq!(
            owner.dispatch("server-generation", &callback()).await,
            Err(SovereignCallbackError::Replay)
        );
        let snapshots = owner.snapshots();
        assert_eq!(snapshots[0].execution_count, 0);
        assert_eq!(snapshots[0].state, CallbackState::Hold);
        assert!(!format!("{snapshots:?}").contains("fixture-private-capability"));
    }

    #[tokio::test]
    async fn disconnect_retains_owner_and_reconnect_cannot_retarget_generation() {
        let root = repository().await;
        let (handle, mut commands) = handle();
        let mut owner = owner(handle.clone(), &root);
        let gate = std::sync::Arc::new(tokio::sync::Notify::new());
        owner.set_test_completion_gate(gate.clone());
        let responder = tokio::spawn(async move {
            let Some(ClientCommand::Request { response_tx, .. }) = commands.recv().await else {
                panic!("status read");
            };
            let _ = response_tx.send(Ok(Ok(serde_json::json!({"status":"running"}))));
        });
        assert!(
            owner
                .dispatch("server-generation", &callback())
                .await
                .expect("dispatch admitted")
        );
        responder.await.expect("status response");
        owner.disconnected();
        assert_eq!(owner.snapshots()[0].state, CallbackState::CleanupPending);
        assert_eq!(
            owner.reconnect("new-server", handle),
            Err(SovereignCallbackError::WrongGeneration)
        );
        gate.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if owner.poll().await[0].state == CallbackState::AckPending {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cleanup retained offline");
        assert_eq!(owner.snapshots()[0].execution_count, 1);
        assert_eq!(
            owner.calls[0]
                .outbox
                .as_ref()
                .expect("retained ack")
                .cleanup_complete,
            true
        );
    }

    #[tokio::test]
    async fn failed_admission_recovers_cleanup_only_without_later_execution() {
        let root = repository().await;
        let (handle, mut commands) = handle();
        let mut owner = owner(handle, &root);
        let responder = tokio::spawn(async move {
            let Some(ClientCommand::Request { response_tx, .. }) = commands.recv().await else {
                panic!("initial status read");
            };
            let _ = response_tx.send(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "fixture lost read",
            )));
            let Some(ClientCommand::Request { response_tx, .. }) = commands.recv().await else {
                panic!("recovery status read");
            };
            let _ = response_tx.send(Ok(Ok(serde_json::json!({"status":"running"}))));
            let Some(ClientCommand::Request {
                request,
                response_tx,
            }) = commands.recv().await
            else {
                panic!("cleanup-only ack");
            };
            let ClientRequest::DynamicToolCleanupAck { params, .. } = *request else {
                panic!("no execution replay");
            };
            assert!(params.response.cleanup_complete && !params.response.success);
            let _ = response_tx.send(Ok(Ok(serde_json::json!({"alreadyAcknowledged":false}))));
        });
        assert_eq!(
            owner.dispatch("server-generation", &callback()).await,
            Err(SovereignCallbackError::ServerStateUnconfirmed)
        );
        let progress = owner.poll().await;
        assert_eq!(progress[0].state, CallbackState::Acknowledged);
        assert_eq!(progress[0].execution_count, 0);
        assert_eq!(progress[0].process_id, None);
        assert_eq!(
            owner.dispatch("server-generation", &callback()).await,
            Err(SovereignCallbackError::Replay)
        );
        responder.await.expect("cleanup-only recovery");
    }

    fn callback_number(index: i64) -> ServerRequest {
        let mut request = callback();
        let ServerRequest::DynamicToolCall { request_id, params } = &mut request else {
            unreachable!();
        };
        *request_id = RequestId::Integer(index);
        params.call_id = format!("call-{index}");
        params.cleanup_token = Some(format!("fixture-private-capability-{index}"));
        request
    }

    fn serve_running_cleanups(mut commands: mpsc::Receiver<ClientCommand>) -> JoinHandle<usize> {
        tokio::spawn(async move {
            let mut acknowledgements = 0;
            while let Some(ClientCommand::Request {
                request,
                response_tx,
            }) = commands.recv().await
            {
                let response = match *request {
                    ClientRequest::DynamicToolCleanupRead { .. } => {
                        serde_json::json!({"status":"running"})
                    }
                    ClientRequest::DynamicToolCleanupAck { params, .. } => {
                        assert!(params.response.cleanup_complete);
                        acknowledgements += 1;
                        serde_json::json!({"alreadyAcknowledged":false})
                    }
                    _ => panic!("only cleanup/status requests are admitted"),
                };
                let _ = response_tx.send(Ok(Ok(response)));
            }
            acknowledgements
        })
    }

    #[tokio::test]
    async fn cancellation_before_dispatch_acknowledges_without_starting_operation() {
        let root = repository().await;
        let (handle, mut commands) = handle();
        let mut owner = owner(handle, &root);
        let responder = tokio::spawn(async move {
            for _ in 0..2 {
                let Some(ClientCommand::Request {
                    request,
                    response_tx,
                }) = commands.recv().await
                else {
                    panic!("status read");
                };
                assert!(matches!(
                    *request,
                    ClientRequest::DynamicToolCleanupRead { .. }
                ));
                let _ = response_tx.send(Ok(Ok(serde_json::json!({"status":"cancelling"}))));
            }
            let Some(ClientCommand::Request {
                request,
                response_tx,
            }) = commands.recv().await
            else {
                panic!("no-execution acknowledgement");
            };
            let ClientRequest::DynamicToolCleanupAck { params, .. } = *request else {
                panic!("cleanup-only delivery");
            };
            assert!(params.response.cleanup_complete && !params.response.success);
            let _ = response_tx.send(Ok(Ok(serde_json::json!({"alreadyAcknowledged":false}))));
        });
        assert!(
            owner
                .dispatch("server-generation", &callback())
                .await
                .expect("consumed cancellation")
        );
        let progress = owner.poll().await;
        assert_eq!(progress[0].state, CallbackState::Acknowledged);
        assert_eq!(progress[0].execution_count, 0);
        assert_eq!(progress[0].process_id, None);
        responder.await.expect("cleanup acknowledged");
    }

    #[tokio::test]
    async fn acknowledged_callback_settles_and_detached_callback_stays_hold_without_execution() {
        let root = repository().await;
        let (handle, mut commands) = handle();
        let mut owner = owner(handle, &root);
        let responder = tokio::spawn(async move {
            for status in ["acknowledged", "ownerDetached"] {
                let Some(ClientCommand::Request { response_tx, .. }) = commands.recv().await else {
                    panic!("status read");
                };
                let _ = response_tx.send(Ok(Ok(serde_json::json!({"status":status}))));
            }
        });
        assert!(
            owner
                .dispatch("server-generation", &callback_number(1))
                .await
                .expect("settled identity")
        );
        assert_eq!(
            owner
                .dispatch("server-generation", &callback_number(2))
                .await,
            Err(SovereignCallbackError::ServerStateUnconfirmed)
        );
        let progress = owner.poll().await;
        assert_eq!(progress[0].state, CallbackState::Acknowledged);
        assert_eq!(progress[1].state, CallbackState::Hold);
        assert!(
            progress
                .iter()
                .all(|call| call.execution_count == 0 && call.process_id.is_none())
        );
        assert!(owner.calls.iter().all(|call| call.outbox.is_none()));
        responder.await.expect("statuses delivered");
    }

    #[tokio::test]
    async fn sixty_five_sequential_calls_retain_replay_guards_without_exhausting_active_capacity() {
        let root = repository().await;
        let (handle, commands) = handle();
        let mut owner = owner(handle, &root);
        let responder = serve_running_cleanups(commands);
        for index in 0..65 {
            assert!(
                owner
                    .dispatch("server-generation", &callback_number(index))
                    .await
                    .expect("admitted sequential call")
            );
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if owner.poll().await.last().expect("retained call").state
                        == CallbackState::Acknowledged
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("sequential call settled");
        }
        let progress = owner.snapshots();
        assert_eq!(progress.len(), 65);
        assert!(
            progress
                .iter()
                .all(|call| call.state == CallbackState::Acknowledged
                    && call.execution_count == 1
                    && call.reaped)
        );
        assert_eq!(
            owner
                .dispatch("server-generation", &callback_number(0))
                .await,
            Err(SovereignCallbackError::Replay)
        );
        drop(owner);
        assert_eq!(responder.await.expect("cleanup transport closed"), 65);
    }

    #[tokio::test]
    async fn sixty_fifth_active_call_retains_no_execution_cleanup_until_acknowledged() {
        let root = repository().await;
        let (handle, commands) = handle();
        let mut owner = owner(handle, &root);
        let responder = serve_running_cleanups(commands);
        let gate = SpawnGate {
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        };
        owner.spawn_gate = Some(gate.clone());
        for index in 0..64 {
            assert!(
                owner
                    .dispatch("server-generation", &callback_number(index))
                    .await
                    .expect("active call admitted")
            );
            tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
                .await
                .expect("worker reached spawn gate");
        }
        assert!(
            owner
                .dispatch("server-generation", &callback_number(64))
                .await
                .expect("overflow identity retained")
        );
        let overflow = &owner.snapshots()[64];
        assert_eq!(overflow.state, CallbackState::AckPending);
        assert_eq!(overflow.execution_count, 0);
        assert_eq!(overflow.process_id, None);
        assert!(
            !owner.calls[64]
                .outbox
                .as_ref()
                .expect("no-execution outcome")
                .success
        );
        assert_eq!(owner.poll().await[64].state, CallbackState::Acknowledged);
        owner.disconnected();
        gate.release.notify_waiters();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let progress = owner.poll().await;
                if progress[..64]
                    .iter()
                    .all(|call| call.state == CallbackState::AckPending)
                {
                    assert!(progress.iter().all(|call| call.process_id.is_none()));
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retained workers cooperatively stopped before spawning");
        drop(owner);
        assert_eq!(responder.await.expect("cleanup transport closed"), 1);
    }

    #[tokio::test]
    async fn pinned_directories_survive_root_and_git_symlink_substitution_before_spawn() {
        use std::os::unix::fs::symlink;
        let root = repository().await;
        let replacement = repository().await;
        let moved = tempfile::tempdir().expect("fixture rename destination");
        let head_reference =
            std::fs::read_to_string(root.path().join(".git/HEAD")).expect("fixture HEAD");
        let expected = std::fs::read_to_string(
            root.path().join(".git").join(
                head_reference
                    .trim()
                    .strip_prefix("ref: ")
                    .expect("symbolic fixture HEAD"),
            ),
        )
        .expect("original commit");
        std::fs::write(
            replacement.path().join(".git/HEAD"),
            "2222222222222222222222222222222222222222\n",
        )
        .expect("distinct replacement HEAD");
        let (handle, _commands) = handle();
        let owner = owner(handle, &root);
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let (observation_tx, observation_rx) = watch::channel(Observation::default());
        let gate = SpawnGate {
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        };
        let worker = tokio::spawn(execute_read_head(
            owner.root.clone(),
            cancel_rx,
            observation_tx,
            None,
            None,
            Some(gate.clone()),
        ));
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .expect("pin-to-spawn window");
        std::fs::rename(root.path().join(".git"), root.path().join(".git.saved"))
            .expect("rename admitted git directory");
        symlink(replacement.path().join(".git"), root.path().join(".git"))
            .expect("substitute git symlink");
        let preserved = moved.path().join("admitted");
        std::fs::rename(root.path(), &preserved).expect("rename admitted root");
        symlink(replacement.path(), root.path()).expect("substitute root symlink");
        gate.release.notify_one();
        let response = tokio::time::timeout(Duration::from_secs(3), worker)
            .await
            .expect("bounded read")
            .expect("worker joined")
            .expect("cleanup confirmed");
        assert!(response.success && response.cleanup_complete);
        let [DynamicToolCallOutputContentItem::InputText { text }] =
            response.content_items.as_slice()
        else {
            panic!("single bounded output");
        };
        let output: serde_json::Value = serde_json::from_str(text).expect("head JSON");
        assert_eq!(output["head"].as_str(), Some(expected.trim()));
        assert!(observation_rx.borrow().reaped);
        std::fs::remove_file(root.path()).expect("remove fixture root symlink");
        std::fs::rename(&preserved, root.path()).expect("restore fixture root");
        std::fs::remove_file(root.path().join(".git")).expect("remove fixture git symlink");
        std::fs::rename(root.path().join(".git.saved"), root.path().join(".git"))
            .expect("restore fixture git directory");
    }
}
