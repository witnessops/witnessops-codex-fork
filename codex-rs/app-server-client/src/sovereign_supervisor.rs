//! Independent callback lifecycle pump for explicitly admitted native sessions.
//! Transport workers keep forwarding RPC responses while this owner waits for
//! child cleanup; foreground interrupt/shutdown requests never drive its clock.
use crate::AppServerEvent;
use crate::AppServerRequestHandle;
use crate::RequestResult;
use crate::sovereign_callback::CallbackProgress;
use crate::sovereign_callback::CallbackState;
use crate::sovereign_callback::SovereignCallbackClient;
use crate::sovereign_callback::SovereignCallbackError;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;
use codex_app_server_protocol::TurnInterruptParams;
use codex_app_server_protocol::TurnInterruptResponse;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

const OWNER_CWD: &str = "/home/karol/WitnessOps/WitnessOps/repos/witnessops-codex-ops";
const OWNER_MODEL: &str = "gpt-5.6-terra";
const OWNER_PROFILE: &str = "wops-codex-ops-read";

pub(crate) fn validate_native_root(root: &std::path::Path) -> io::Result<()> {
    if root.as_os_str() != std::ffi::OsStr::new(OWNER_CWD) {
        return Err(io::Error::other(
            "Sovereign native owner requires its fixed absolute repository root",
        ));
    }
    Ok(())
}

fn validate_thread_scope(value: &serde_json::Value) -> io::Result<()> {
    let expected = Some(OWNER_CWD);
    if value.get("cwd").and_then(serde_json::Value::as_str) != expected
        || value
            .pointer("/thread/cwd")
            .and_then(serde_json::Value::as_str)
            != expected
        || value.get("model").and_then(serde_json::Value::as_str) != Some(OWNER_MODEL)
        || value
            .pointer("/thread/model")
            .and_then(serde_json::Value::as_str)
            != Some(OWNER_MODEL)
        || value
            .pointer("/activePermissionProfile/id")
            .and_then(serde_json::Value::as_str)
            != Some(OWNER_PROFILE)
    {
        return Err(io::Error::other(
            "Sovereign thread response does not match the admitted owner scope",
        ));
    }
    Ok(())
}

const READY: u8 = 0;
const DISCONNECTED: u8 = 1;
const RECOVERY_ONLY: u8 = 2;
const CLOSING: u8 = 3;
const HOLD: u8 = 4;
static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) struct Attachment {
    owner: SovereignCallbackSupervisor,
    epoch: u64,
}
pub(crate) type SovereignSlot = Arc<Mutex<Option<Attachment>>>;
pub(crate) type WeakSovereignSlot = Weak<Mutex<Option<Attachment>>>;
pub(crate) fn slot() -> SovereignSlot {
    Arc::new(Mutex::new(None))
}
pub(crate) fn get(slot: &WeakSovereignSlot) -> Option<SovereignCallbackSupervisor> {
    let slot = slot.upgrade()?;
    let guard = slot.lock().ok()?;
    // Keep stale transports guarded: they must never fall back to ordinary
    // callback execution or bypass recovery-only request admission.
    guard.as_ref().map(|attachment| attachment.owner.clone())
}
pub(crate) fn owner(slot: &SovereignSlot) -> Option<SovereignCallbackSupervisor> {
    let guard = slot.lock().ok()?;
    let attachment = guard.as_ref()?;
    (attachment.epoch == attachment.owner.inner.epoch.load(Ordering::Acquire))
        .then(|| attachment.owner.clone())
}
pub(crate) fn install(slot: &SovereignSlot, owner: SovereignCallbackSupervisor) -> io::Result<()> {
    let mut guard = slot.lock().map_err(|_| hold_error())?;
    if guard.is_some() {
        return Err(io::Error::other(
            "Sovereign callback owner is already installed",
        ));
    }
    let epoch = owner.inner.epoch.load(Ordering::Acquire);
    let mut owner = owner;
    owner.binding_epoch = epoch;
    *guard = Some(Attachment { owner, epoch });
    Ok(())
}

fn hold_error() -> io::Error {
    io::Error::other("Sovereign callback cleanup is HOLD; original execution will not be replayed")
}

#[derive(Clone, Debug, Default)]
pub struct SovereignSupervisorStatus {
    pub calls: Vec<CallbackProgress>,
    pub hold: bool,
    pub recovery_only: bool,
}

/// Cloneable lifetime owner retained across transport replacement. It contains
/// no executable callback closure and exposes no private recovery capability.
#[derive(Clone)]
pub struct SovereignCallbackSupervisor {
    inner: Arc<SupervisorInner>,
    binding_epoch: u64,
    native_root_bound: bool,
}
struct SupervisorInner {
    commands: mpsc::UnboundedSender<Command>,
    status: watch::Receiver<SovereignSupervisorStatus>,
    phase: Arc<AtomicU8>,
    threads: Arc<Mutex<HashSet<String>>>,
    endpoint: String,
    epoch: Arc<AtomicU64>,
    inflight: Arc<AtomicUsize>,
    turns: Arc<Mutex<Turns>>,
    execution_guard: Arc<Mutex<bool>>,
}
#[derive(Default)]
struct Turns {
    active: HashMap<String, String>,
    completed: HashSet<(String, String)>,
}
impl Turns {
    fn started(&mut self, thread: String, turn: String) {
        if !self.completed.contains(&(thread.clone(), turn.clone())) {
            self.active.insert(thread, turn);
        }
    }
    fn completed(&mut self, thread: String, turn: String) {
        if self.active.get(&thread) == Some(&turn) {
            self.active.remove(&thread);
        }
        self.completed.insert((thread, turn));
    }
}
pub(crate) struct ExecutionLease {
    inflight: Arc<AtomicUsize>,
    thread: Option<String>,
}
impl Drop for ExecutionLease {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

enum Command {
    Admit(String, oneshot::Sender<io::Result<()>>),
    Request(Box<ServerRequest>, u64),
    Notification(Box<ServerNotification>),
    Disconnected(u64),
    Drain(oneshot::Sender<()>),
    Reconnect(AppServerRequestHandle, oneshot::Sender<io::Result<()>>),
}

impl SovereignCallbackSupervisor {
    pub(crate) fn start(
        handle: AppServerRequestHandle,
        root: PathBuf,
        endpoint: String,
    ) -> io::Result<Self> {
        validate_native_root(&root)?;
        Self::start_inner(handle, root, endpoint, true)
    }

    fn start_inner(
        handle: AppServerRequestHandle,
        root: PathBuf,
        endpoint: String,
        native_root_bound: bool,
    ) -> io::Result<Self> {
        let generation = format!(
            "native-owner-{}-{}",
            std::process::id(),
            NEXT_OWNER.fetch_add(1, Ordering::Relaxed)
        );
        let mut adapter =
            SovereignCallbackClient::new_unassigned(handle, generation.clone(), root)?;
        let execution_guard = Arc::new(Mutex::new(true));
        adapter.set_execution_guard(Arc::clone(&execution_guard));
        let (commands, receive) = mpsc::unbounded_channel();
        let (status_tx, status) = watch::channel(SovereignSupervisorStatus::default());
        let phase = Arc::new(AtomicU8::new(READY));
        let threads = Arc::new(Mutex::new(HashSet::new()));
        let inflight = Arc::new(AtomicUsize::new(0));
        let turns = Arc::new(Mutex::new(Turns::default()));
        let epoch = Arc::new(AtomicU64::new(1));
        tokio::spawn(run(
            adapter,
            generation,
            receive,
            status_tx,
            Arc::clone(&phase),
            Arc::clone(&threads),
            Arc::clone(&inflight),
            Arc::clone(&turns),
            Arc::clone(&epoch),
        ));
        Ok(Self {
            inner: Arc::new(SupervisorInner {
                commands,
                status,
                phase,
                threads,
                endpoint,
                epoch,
                inflight,
                turns,
                execution_guard,
            }),
            binding_epoch: 1,
            native_root_bound,
        })
    }

    fn revoke_execution(&self) {
        if let Ok(mut permitted) = self.inner.execution_guard.lock() {
            *permitted = false;
        }
    }

    pub(crate) fn validate_native_binding(&self) -> io::Result<()> {
        if self.native_root_bound {
            Ok(())
        } else {
            Err(hold_error())
        }
    }

    pub fn status(&self) -> SovereignSupervisorStatus {
        self.inner.status.borrow().clone()
    }

    /// Use before replacing a transport. The same owner and immutable outbox
    /// remain live; only cooperative cancellation is requested.
    pub fn disconnected(&self) {
        if self.binding_epoch != self.inner.epoch.load(Ordering::Acquire) {
            return;
        }
        self.revoke_execution();
        self.inner.phase.store(DISCONNECTED, Ordering::Release);
        let _ = self
            .inner
            .commands
            .send(Command::Disconnected(self.binding_epoch));
    }

    pub async fn drain(&self) -> io::Result<()> {
        self.revoke_execution();
        let _ =
            self.inner
                .phase
                .compare_exchange(READY, CLOSING, Ordering::AcqRel, Ordering::Acquire);
        let (send, receive) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Drain(send))
            .map_err(|_| hold_error())?;
        tokio::time::timeout(DRAIN_TIMEOUT, receive)
            .await
            .map_err(|_| hold_error())?
            .map_err(|_| hold_error())
    }

    pub(crate) async fn reconnect(
        &self,
        endpoint: &str,
        handle: AppServerRequestHandle,
    ) -> io::Result<()> {
        if endpoint != self.inner.endpoint
            || !matches!(
                self.inner.phase.load(Ordering::Acquire),
                DISCONNECTED | HOLD
            )
        {
            return Err(hold_error());
        }
        self.revoke_execution();
        {
            // Serialize attachment replacement with history reconciliation so a
            // response cannot pass its epoch check and mutate the next owner.
            let _turns = self.inner.turns.lock().map_err(|_| hold_error())?;
            self.inner.epoch.fetch_add(1, Ordering::AcqRel);
        }
        self.inner.phase.store(RECOVERY_ONLY, Ordering::Release);
        let (send, receive) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Reconnect(handle, send))
            .map_err(|_| hold_error())?;
        tokio::time::timeout(DRAIN_TIMEOUT, receive)
            .await
            .map_err(|_| hold_error())?
            .map_err(|_| hold_error())?
    }

    pub(crate) fn feed_request(&self, request: &ServerRequest) -> bool {
        if !matches!(request, ServerRequest::DynamicToolCall { params, .. } if params.namespace.as_deref() == Some("witnessops"))
        {
            return false;
        }
        if let ServerRequest::DynamicToolCall { params, .. } = request
            && self
                .inner
                .threads
                .lock()
                .is_ok_and(|known| known.contains(&params.thread_id))
            && let Ok(mut turns) = self.inner.turns.lock()
        {
            turns.started(params.thread_id.clone(), params.turn_id.clone());
        }
        if self
            .inner
            .commands
            .send(Command::Request(
                Box::new(request.clone()),
                self.binding_epoch,
            ))
            .is_err()
        {
            self.inner.phase.store(HOLD, Ordering::Release);
        }
        true
    }
    pub(crate) fn feed_notification(&self, notification: &ServerNotification) {
        if self.binding_epoch != self.inner.epoch.load(Ordering::Acquire) {
            return;
        }
        if let Ok(mut turns) = self.inner.turns.lock() {
            match notification {
                ServerNotification::TurnStarted(item)
                    if self
                        .inner
                        .threads
                        .lock()
                        .is_ok_and(|known| known.contains(&item.thread_id)) =>
                {
                    turns.started(item.thread_id.clone(), item.turn.id.clone())
                }
                ServerNotification::TurnCompleted(item) => {
                    turns.completed(item.thread_id.clone(), item.turn.id.clone())
                }
                _ => {}
            }
        }

        if self
            .inner
            .commands
            .send(Command::Notification(Box::new(notification.clone())))
            .is_err()
        {
            self.inner.phase.store(HOLD, Ordering::Release);
        }
    }
    pub(crate) fn feed_event(&self, event: &AppServerEvent) -> bool {
        match event {
            AppServerEvent::ServerRequest(request) => self.feed_request(request),
            AppServerEvent::ServerNotification(notification) => {
                self.feed_notification(notification);
                false
            }
            AppServerEvent::Disconnected { .. } => {
                self.disconnected();
                false
            }
            AppServerEvent::Lagged { .. } => {
                self.disconnected();
                false
            }
        }
    }

    pub(crate) fn check_method(
        &self,
        method: &str,
        params: Option<&serde_json::Value>,
    ) -> io::Result<()> {
        if self.binding_epoch != self.inner.epoch.load(Ordering::Acquire) {
            return Err(hold_error());
        }
        let phase = self.inner.phase.load(Ordering::Acquire);
        if phase == READY {
            if matches!(method, "turn/start" | "turn/steer") {
                let thread = params
                    .and_then(|value| value.get("threadId"))
                    .and_then(serde_json::Value::as_str);
                if !thread.is_some_and(|id| {
                    self.inner
                        .threads
                        .lock()
                        .is_ok_and(|known| known.contains(id))
                }) {
                    return Err(hold_error());
                }
            }
            if method == "review/start" {
                return Err(io::Error::other(
                    "Sovereign read_head admission does not include review turns",
                ));
            }
            return Ok(());
        }
        if matches!(
            method,
            "item/tool/cleanup/read"
                | "item/tool/cleanup/ack"
                | "turn/interrupt"
                | "thread/unsubscribe"
        ) {
            return Ok(());
        }
        if phase != RECOVERY_ONLY {
            return Err(hold_error());
        }
        if method == "thread/resume"
            && params
                .and_then(serde_json::Value::as_object)
                .is_none_or(|params| {
                    params.iter().any(|(key, value)| {
                        !matches!(
                            key.as_str(),
                            "threadId" | "excludeTurns" | "initialTurnsPage"
                        ) && (key == "serviceTier" || !value.is_null())
                    })
                })
        {
            return Err(hold_error());
        }
        if matches!(
            method,
            "thread/resume"
                | "thread/read"
                | "thread/turns/list"
                | "thread/items/list"
                | "thread/attachment/list"
                | "thread/goal/get"
        ) {
            let thread = params
                .and_then(|value| value.get("threadId"))
                .and_then(serde_json::Value::as_str);
            if thread.is_some_and(|id| {
                self.inner
                    .threads
                    .lock()
                    .is_ok_and(|known| known.contains(id))
            }) {
                return Ok(());
            }
            return Err(hold_error());
        }
        if !matches!(
            method,
            "thread/read"
                | "thread/list"
                | "thread/loaded/list"
                | "thread/turns/list"
                | "thread/items/list"
                | "config/read"
                | "configRequirements/read"
                | "account/read"
                | "account/rateLimits/read"
                | "model/list"
                | "collaborationMode/list"
                | "thread/attachment/list"
                | "thread/goal/get"
                | "skills/list"
                | "mcpServerStatus/list"
                | "app/list"
        ) {
            return Err(hold_error());
        }
        Ok(())
    }
    pub(crate) fn check_request(&self, request: &ClientRequest) -> io::Result<()> {
        let value = serde_json::to_value(request).map_err(|_| hold_error())?;
        self.check_method(request.method_name(), value.get("params"))
    }
    pub(crate) fn begin_execution(
        &self,
        method: &str,
        params: Option<&serde_json::Value>,
    ) -> Option<ExecutionLease> {
        if !matches!(
            method,
            "thread/start"
                | "thread/resume"
                | "thread/fork"
                | "thread/read"
                | "thread/turns/list"
                | "turn/start"
                | "review/start"
        ) {
            return None;
        }
        self.inner.inflight.fetch_add(1, Ordering::AcqRel);
        Some(ExecutionLease {
            inflight: Arc::clone(&self.inner.inflight),
            thread: params
                .and_then(|value| value.get("threadId"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
        })
    }

    fn reconcile_terminal_turns(
        &self,
        method: &str,
        value: &serde_json::Value,
        execution: Option<&ExecutionLease>,
    ) -> io::Result<()> {
        let Some(thread) = execution.and_then(|lease| lease.thread.as_ref()) else {
            return Ok(());
        };
        // History may be partial or paginated. It is evidence only for the
        // admitted thread named by this request, never for an omitted turn.
        let admitted = self
            .inner
            .threads
            .lock()
            .map_err(|_| hold_error())?
            .contains(thread);
        if !admitted {
            return Ok(());
        }
        let reported = match method {
            "thread/resume" | "thread/read" => {
                if value
                    .pointer("/thread/id")
                    .and_then(serde_json::Value::as_str)
                    != Some(thread.as_str())
                {
                    return Err(hold_error());
                }
                value.pointer("/thread/turns")
            }
            "thread/turns/list" => value.get("data"),
            _ => return Ok(()),
        };
        let mut turns = self.inner.turns.lock().map_err(|_| hold_error())?;
        if self.binding_epoch != self.inner.epoch.load(Ordering::Acquire) {
            return Err(hold_error());
        }
        for turn in reported
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            if matches!(
                turn.get("status").and_then(serde_json::Value::as_str),
                Some("completed" | "interrupted" | "failed")
            ) && let Some(id) = turn.get("id").and_then(serde_json::Value::as_str)
                && turns.active.get(thread).is_some_and(|active| active == id)
            {
                // This settles only the retained turn. Callback cleanup still
                // requires its own exact acknowledgement before drain succeeds.
                turns.completed(thread.clone(), id.to_owned());
            }
        }
        Ok(())
    }

    pub(crate) async fn response(
        &self,
        method: &str,
        result: &io::Result<RequestResult>,
        execution: Option<&ExecutionLease>,
    ) -> io::Result<()> {
        if self.binding_epoch != self.inner.epoch.load(Ordering::Acquire) {
            return Err(hold_error());
        }
        if method == "turn/start" {
            if let (Some(thread), Ok(Ok(value))) =
                (execution.and_then(|lease| lease.thread.as_ref()), result)
                && let Some(turn) = value
                    .get("turn")
                    .and_then(|value| value.get("id"))
                    .and_then(serde_json::Value::as_str)
                && let Ok(mut turns) = self.inner.turns.lock()
            {
                turns.started(thread.clone(), turn.to_owned());
            }
            return Ok(());
        }
        if matches!(method, "thread/read" | "thread/turns/list") {
            if let Ok(Ok(value)) = result {
                self.reconcile_terminal_turns(method, value, execution)?;
            }
            return Ok(());
        }
        if !matches!(method, "thread/start" | "thread/resume" | "thread/fork") {
            return Ok(());
        }
        let Ok(Ok(value)) = result else {
            return Ok(());
        };
        if let Err(error) = validate_thread_scope(value) {
            self.revoke_execution();
            self.inner.phase.store(HOLD, Ordering::Release);
            let (done, _) = oneshot::channel();
            let _ = self.inner.commands.send(Command::Drain(done));
            return Err(error);
        }
        let thread = value
            .get("thread")
            .and_then(|value| value.get("id"))
            .and_then(serde_json::Value::as_str)
            .ok_or_else(hold_error)?;
        if method == "thread/resume"
            && let Some(requested) = execution.and_then(|lease| lease.thread.as_ref())
            && requested != thread
        {
            return Err(hold_error());
        }
        let (send, receive) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Admit(thread.to_owned(), send))
            .map_err(|_| hold_error())?;
        receive.await.map_err(|_| hold_error())??;
        {
            let mut turns = self.inner.turns.lock().map_err(|_| hold_error())?;
            // Admission awaits the lifecycle pump; reconnect may have replaced
            // this attachment while that response was suspended.
            if self.binding_epoch != self.inner.epoch.load(Ordering::Acquire) {
                return Err(hold_error());
            }
            if let Some(active) = value
                .get("thread")
                .and_then(|value| value.get("turns"))
                .and_then(serde_json::Value::as_array)
            {
                for turn in active {
                    if turn.get("status").and_then(serde_json::Value::as_str) == Some("inProgress")
                        && let Some(id) = turn.get("id").and_then(serde_json::Value::as_str)
                    {
                        turns.started(thread.to_owned(), id.to_owned());
                    }
                }
            }
        }
        if method == "thread/resume" {
            self.reconcile_terminal_turns(method, value, execution)?;
        }
        Ok(())
    }
}

async fn run(
    mut owner: SovereignCallbackClient,
    generation: String,
    mut commands: mpsc::UnboundedReceiver<Command>,
    status: watch::Sender<SovereignSupervisorStatus>,
    phase: Arc<AtomicU8>,
    threads: Arc<Mutex<HashSet<String>>>,
    inflight: Arc<AtomicUsize>,
    turns: Arc<Mutex<Turns>>,
    epoch: Arc<AtomicU64>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(20));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut drains = Vec::new();
    let mut input_closed = false;
    let mut explicit_hold = false;
    let mut closing_requested = false;
    let mut interrupted = HashMap::new();
    let (interrupt_result, mut interrupt_results) =
        mpsc::unbounded_channel::<((String, String), u64, bool)>();
    loop {
        tokio::select! {
            biased;
            command = commands.recv(), if !input_closed => {
                match command {
                    Some(Command::Admit(thread, done)) => {
                        let result = if matches!(phase.load(Ordering::Acquire), READY | CLOSING) || threads.lock().is_ok_and(|known| known.contains(&thread)) {
                            owner.admit_thread(thread.clone()).map_err(|_| hold_error())
                        } else { Err(hold_error()) };
                        if result.is_ok() { if let Ok(mut known) = threads.lock() { known.insert(thread); } }
                        else { explicit_hold = true; }
                        let _ = done.send(result);
                    }
                    Some(Command::Request(request, connection_epoch)) => {
                        let current = phase.load(Ordering::Acquire);
                        if current == DISCONNECTED { owner.disconnected(); }
                        let result = if current == READY && connection_epoch == epoch.load(Ordering::Acquire) { owner.dispatch(&generation, &request).await }
                            else { owner.decline_execution(&generation, &request).await };
                        // A duplicate can never rerun work; the original retained
                        // outcome remains authoritative. Transport errors retain outbox.
                        if matches!(result, Err(SovereignCallbackError::NotAdmitted | SovereignCallbackError::MissingCapability | SovereignCallbackError::Capacity | SovereignCallbackError::WrongGeneration)) { explicit_hold = true; }
                    }
                    Some(Command::Notification(notification)) => { let _ = owner.observe_notification(&generation, &notification); }
                    Some(Command::Disconnected(connection_epoch)) => {
                        if connection_epoch == epoch.load(Ordering::Acquire) { owner.disconnected(); }
                    }
                    Some(Command::Drain(done)) => { closing_requested = true; owner.request_cleanup(); drains.push(done); }
                    Some(Command::Reconnect(handle, done)) => {
                        let result = owner.reconnect_verified(handle).await.map_err(|_| hold_error());
                        if result.is_err() || done.is_closed() {
                            owner.disconnected();
                            phase.store(HOLD, Ordering::Release);
                        } else {
                            interrupted.clear();
                            phase.store(RECOVERY_ONLY, Ordering::Release);
                        }
                        let _ = done.send(result);
                    }
                    None => { input_closed = true; closing_requested = true; owner.request_cleanup(); }
                }
            }
            Some((key, attempt, success)) = interrupt_results.recv() => {
                if interrupted.get(&key) == Some(&attempt) {
                    // Failed stop requests retain their intent. Only a verified
                    // reconnect clears the attempt and permits cleanup-only retry.
                    if success { interrupted.remove(&key); }
                }
            }
            _ = tick.tick() => { owner.poll().await; }
        }
        if closing_requested
            && matches!(
                phase.load(Ordering::Acquire),
                READY | CLOSING | RECOVERY_ONLY
            )
            && inflight.load(Ordering::Acquire) == 0
        {
            owner.request_cleanup();
            let active = turns
                .lock()
                .map(|turns| turns.active.clone())
                .unwrap_or_default();
            for (thread_id, turn_id) in active {
                let key = (thread_id.clone(), turn_id.clone());
                if interrupted.contains_key(&key) {
                    continue;
                }
                let attempt = NEXT_OWNER.fetch_add(1, Ordering::Relaxed);
                interrupted.insert(key.clone(), attempt);
                let handle = owner.request_handle();
                let turns = Arc::clone(&turns);
                let result_tx = interrupt_result.clone();
                tokio::spawn(async move {
                    let request = ClientRequest::TurnInterrupt {
                        request_id: RequestId::String(format!("native-cleanup-stop-{attempt}")),
                        params: TurnInterruptParams {
                            thread_id: thread_id.clone(),
                            turn_id: turn_id.clone(),
                        },
                    };
                    let success = handle
                        .request_typed::<TurnInterruptResponse>(request)
                        .await
                        .is_ok();
                    if success && let Ok(mut turns) = turns.lock() {
                        turns.completed(thread_id, turn_id);
                    }
                    let _ = result_tx.send((key, attempt, success));
                });
            }
        }
        let calls = owner.snapshots();
        let settled = !explicit_hold
            && inflight.load(Ordering::Acquire) == 0
            && turns.lock().is_ok_and(|turns| turns.active.is_empty())
            && calls
                .iter()
                .all(|call| call.state == CallbackState::Acknowledged);
        let held = explicit_hold
            || phase.load(Ordering::Acquire) == HOLD
            || calls.iter().any(|call| call.state == CallbackState::Hold);
        if explicit_hold {
            phase.store(HOLD, Ordering::Release);
        }
        let _ = status.send(SovereignSupervisorStatus {
            calls,
            hold: held,
            recovery_only: phase.load(Ordering::Acquire) == RECOVERY_ONLY,
        });
        // A bounded drain caller may time out while the retained owner remains
        // in HOLD. Keep only live waiters across cleanup-only retry attempts.
        drains.retain(|done| !done.is_closed());
        if settled {
            for done in drains.drain(..) {
                let _ = done.send(());
            }
        }
        if input_closed && settled {
            break;
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::ClientCommand;
    use crate::InProcessAppServerRequestHandle;
    use codex_app_server_protocol::DynamicToolCallParams;
    use pretty_assertions::assert_eq;

    fn fixture() -> (
        tempfile::TempDir,
        AppServerRequestHandle,
        mpsc::Receiver<ClientCommand>,
    ) {
        let root = tempfile::tempdir().unwrap();
        let git = root.path().join(".git");
        std::fs::create_dir_all(git.join("refs/heads")).unwrap();
        std::fs::create_dir_all(git.join("objects")).unwrap();
        std::fs::write(git.join("HEAD"), "ref: refs/heads/main\n").unwrap();
        std::fs::write(
            git.join("refs/heads/main"),
            "0123456789abcdef0123456789abcdef01234567\n",
        )
        .unwrap();
        std::fs::write(
            git.join("config"),
            "[core]\nrepositoryformatversion = 0\nbare = false\n",
        )
        .unwrap();
        let (command_tx, commands) = mpsc::channel(32);
        (
            root,
            AppServerRequestHandle::InProcess(InProcessAppServerRequestHandle { command_tx }),
            commands,
        )
    }
    fn scope_response() -> serde_json::Value {
        serde_json::json!({
            "thread":{"id":"thread", "turns":[], "cwd":OWNER_CWD, "model":OWNER_MODEL},
            "cwd":OWNER_CWD, "model":OWNER_MODEL,
            "activePermissionProfile":{"id":OWNER_PROFILE, "extends":":read-only"}
        })
    }

    #[test]
    fn native_owner_rejects_other_root_and_mismatched_thread_scope() {
        let (_root, handle, _commands) = fixture();
        assert!(
            SovereignCallbackSupervisor::start(
                handle,
                PathBuf::from("/tmp/other-owner"),
                "fixture".into()
            )
            .is_err()
        );
        assert!(validate_native_root(std::path::Path::new(OWNER_CWD)).is_ok());
        assert!(validate_native_root(std::path::Path::new("relative-owner")).is_err());
        assert!(validate_thread_scope(&scope_response()).is_ok());
        for field in [
            "/cwd",
            "/thread/cwd",
            "/model",
            "/thread/model",
            "/activePermissionProfile/id",
        ] {
            let mut response = scope_response();
            *response.pointer_mut(field).unwrap() = serde_json::json!("other");
            assert!(
                validate_thread_scope(&response).is_err(),
                "scope mismatch admitted: {field}"
            );
        }
    }

    #[tokio::test]
    async fn mismatched_thread_response_never_admits_execution() {
        for method in ["thread/start", "thread/resume", "thread/fork"] {
            let (root, handle, _commands) = fixture();
            let owner = SovereignCallbackSupervisor::start_inner(
                handle,
                root.path().to_owned(),
                "fixture".into(),
                false,
            )
            .unwrap();
            let mut response = scope_response();
            response["cwd"] = serde_json::json!("/tmp/wrong-root");
            assert!(
                owner
                    .response(method, &Ok(Ok(response)), None)
                    .await
                    .is_err()
            );
            assert!(owner.inner.threads.lock().unwrap().is_empty());
            assert!(
                owner
                    .check_method(
                        "turn/start",
                        Some(&serde_json::json!({"threadId":"thread"}))
                    )
                    .is_err()
            );
            assert!(owner.status().calls.is_empty());
            owner.drain().await.unwrap();
        }
    }

    async fn admit(owner: &SovereignCallbackSupervisor) {
        owner
            .response("thread/start", &Ok(Ok(scope_response())), None)
            .await
            .unwrap();
    }
    async fn until(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn closing_admits_already_issued_thread_response_and_waits_for_execution_lease() {
        let (root, handle, _commands) = fixture();
        let owner = SovereignCallbackSupervisor::start_inner(
            handle,
            root.path().to_owned(),
            "fixture".into(),
            false,
        )
        .unwrap();
        let execution = owner.begin_execution("thread/start", None).unwrap();
        assert!(owner.check_method("thread/start", None).is_ok());
        let drain_owner = owner.clone();
        let drain = tokio::spawn(async move { drain_owner.drain().await });
        until(|| owner.inner.phase.load(Ordering::Acquire) == CLOSING).await;
        admit(&owner).await;
        assert!(!drain.is_finished());
        assert!(owner.check_method("turn/start", None).is_err());
        drop(execution);
        tokio::time::timeout(Duration::from_secs(3), drain)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn recovery_is_read_only_and_binds_resume_to_retained_threads() {
        let (root, handle, _commands) = fixture();
        let owner = SovereignCallbackSupervisor::start_inner(
            handle,
            root.path().to_owned(),
            "fixture".into(),
            false,
        )
        .unwrap();
        admit(&owner).await;
        owner.inner.phase.store(RECOVERY_ONLY, Ordering::Release);
        for method in [
            "turn/start",
            "turn/steer",
            "process/spawn",
            "command/exec",
            "thread/fork",
            "thread/start",
            "thread/revert",
            "config/batchWrite",
        ] {
            assert!(
                owner.check_method(method, None).is_err(),
                "execution/mutation method admitted: {method}"
            );
        }
        assert!(owner.check_method("config/read", None).is_ok());
        assert!(
            owner
                .check_method(
                    "thread/resume",
                    Some(&serde_json::json!({"threadId":"thread"}))
                )
                .is_ok()
        );
        assert!(
            owner
                .check_method(
                    "thread/resume",
                    Some(&serde_json::json!({"threadId":"foreign"}))
                )
                .is_err()
        );
        assert!(
            owner
                .check_method(
                    "thread/resume",
                    Some(&serde_json::json!({"threadId":"thread", "model":"other"}))
                )
                .is_err()
        );
        assert!(
            owner
                .check_method(
                    "thread/resume",
                    Some(&serde_json::json!({"threadId":"thread", "serviceTier":null}))
                )
                .is_err()
        );
        for method in [
            "thread/read",
            "thread/turns/list",
            "thread/items/list",
            "thread/attachment/list",
            "thread/goal/get",
        ] {
            assert!(
                owner
                    .check_method(method, Some(&serde_json::json!({"threadId":"thread"})))
                    .is_ok()
            );
            assert!(
                owner
                    .check_method(method, Some(&serde_json::json!({"threadId":"foreign"})))
                    .is_err()
            );
        }
        owner.drain().await.unwrap();
    }

    #[tokio::test]
    async fn superseded_transport_slot_cannot_disconnect_or_admit_on_new_attachment() {
        let (root, handle, _commands) = fixture();
        let owner = SovereignCallbackSupervisor::start_inner(
            handle,
            root.path().to_owned(),
            "fixture".into(),
            false,
        )
        .unwrap();
        let old = slot();
        install(&old, owner.clone()).unwrap();
        owner.inner.epoch.fetch_add(1, Ordering::AcqRel);
        let new = slot();
        install(&new, owner.clone()).unwrap();
        assert!(super::owner(&old).is_none());
        assert!(
            get(&Arc::downgrade(&old))
                .unwrap()
                .check_method("turn/start", None)
                .is_err()
        );
        assert!(super::owner(&new).is_some());
        owner.disconnected();
        assert_eq!(owner.inner.phase.load(Ordering::Acquire), READY);
        assert!(owner.check_method("turn/start", None).is_err());
        assert!(
            owner
                .response(
                    "thread/start",
                    &Ok(Ok(serde_json::json!({"thread":{"id":"late"}}))),
                    None
                )
                .await
                .is_err()
        );
        super::owner(&new).unwrap().drain().await.unwrap();
    }

    #[tokio::test]
    async fn disconnect_during_status_preflight_prevents_process_spawn() {
        let (root, handle, mut commands) = fixture();
        let owner = SovereignCallbackSupervisor::start_inner(
            handle.clone(),
            root.path().to_owned(),
            "fixture".into(),
            false,
        )
        .unwrap();
        admit(&owner).await;
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut first_read = Some((entered_tx, release_rx));
            let mut acknowledged = false;
            while let Some(ClientCommand::Request {
                request,
                response_tx,
            }) = commands.recv().await
            {
                let response = match *request {
                    ClientRequest::DynamicToolCleanupRead { .. } => {
                        if let Some((entered, release)) = first_read.take() {
                            let _ = entered.send(());
                            let _ = release.await;
                        }
                        serde_json::json!({"status":if acknowledged {"acknowledged"} else {"running"}})
                    }
                    ClientRequest::DynamicToolCleanupAck { params, .. } => {
                        assert!(params.response.cleanup_complete);
                        assert!(!params.response.success);
                        acknowledged = true;
                        serde_json::json!({"alreadyAcknowledged":false})
                    }
                    ClientRequest::TurnInterrupt { .. } => serde_json::json!({}),
                    _ => panic!("unexpected fixture RPC"),
                };
                let _ = response_tx.send(Ok(Ok(response)));
            }
        });
        assert!(owner.feed_request(&ServerRequest::DynamicToolCall {
            request_id: RequestId::Integer(2),
            params: DynamicToolCallParams {
                cleanup_token: Some("withheld-preflight-capability".into()),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                call_id: "withheld".into(),
                namespace: Some("witnessops".into()),
                tool: "read_head".into(),
                arguments: serde_json::json!({}),
            },
        }));
        tokio::time::timeout(Duration::from_secs(3), entered_rx)
            .await
            .unwrap()
            .unwrap();
        owner.disconnected();
        release_tx.send(()).unwrap();
        until(|| !owner.status().calls.is_empty()).await;
        assert_eq!(owner.status().calls[0].execution_count, 0);
        assert_eq!(owner.status().calls[0].process_id, None);
        owner.reconnect("fixture", handle.clone()).await.unwrap();
        owner.drain().await.unwrap();
        assert_eq!(owner.status().calls[0].execution_count, 0);
        assert_eq!(owner.status().calls[0].process_id, None);
        drop(owner);
        drop(handle);
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn pending_drain_retains_stop_intent_and_retries_interrupt_only_after_verified_reconnect()
    {
        let (root, handle, mut commands) = fixture();
        let owner = SovereignCallbackSupervisor::start_inner(
            handle.clone(),
            root.path().to_owned(),
            "fixture".into(),
            false,
        )
        .unwrap();
        admit(&owner).await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&attempts);
        let server = tokio::spawn(async move {
            let mut acknowledged = false;
            while let Some(ClientCommand::Request {
                request,
                response_tx,
            }) = commands.recv().await
            {
                let response = match *request {
                    ClientRequest::DynamicToolCleanupRead { .. } => Ok(Ok(
                        serde_json::json!({"status":if acknowledged {"acknowledged"} else {"running"}}),
                    )),
                    ClientRequest::DynamicToolCleanupAck { params, .. } => {
                        assert!(params.response.cleanup_complete);
                        acknowledged = true;
                        Ok(Ok(serde_json::json!({"alreadyAcknowledged":false})))
                    }
                    ClientRequest::TurnInterrupt { .. } => {
                        if observed.fetch_add(1, Ordering::AcqRel) == 0 {
                            Err(io::Error::new(
                                io::ErrorKind::ConnectionReset,
                                "fixture lost interrupt",
                            ))
                        } else {
                            Ok(Ok(serde_json::json!({})))
                        }
                    }
                    _ => panic!("unexpected fixture RPC"),
                };
                let _ = response_tx.send(response);
            }
        });
        let request = ServerRequest::DynamicToolCall {
            request_id: RequestId::Integer(1),
            params: DynamicToolCallParams {
                cleanup_token: Some("private-fixture-capability".into()),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                call_id: "call".into(),
                namespace: Some("witnessops".into()),
                tool: "read_head".into(),
                arguments: serde_json::json!({}),
            },
        };
        assert!(owner.feed_request(&request));
        until(|| {
            owner
                .status()
                .calls
                .first()
                .is_some_and(|call| call.state == CallbackState::Acknowledged)
        })
        .await;
        let draining_owner = owner.clone();
        let draining = tokio::spawn(async move { draining_owner.drain().await });
        until(|| attempts.load(Ordering::Acquire) == 1).await;
        owner.disconnected();
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(!draining.is_finished());
        assert_eq!(attempts.load(Ordering::Acquire), 1);
        owner.reconnect("fixture", handle.clone()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), draining)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(attempts.load(Ordering::Acquire), 2);
        assert_eq!(owner.status().calls[0].execution_count, 1);
        assert!(owner.status().calls[0].reaped);
        drop(owner);
        drop(handle);
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
    }

    fn resume_with_turns(turns: serde_json::Value) -> serde_json::Value {
        let mut response = scope_response();
        response["thread"]["turns"] = turns;
        response
    }

    async fn retained_turn_fixture() -> (
        tempfile::TempDir,
        SovereignCallbackSupervisor,
        mpsc::Receiver<ClientCommand>,
    ) {
        let (root, handle, commands) = fixture();
        let owner = SovereignCallbackSupervisor::start_inner(
            handle,
            root.path().to_owned(),
            "fixture".into(),
            false,
        )
        .unwrap();
        admit(&owner).await;
        owner.inner.phase.store(RECOVERY_ONLY, Ordering::Release);
        owner
            .inner
            .turns
            .lock()
            .unwrap()
            .started("thread".into(), "turn".into());
        (root, owner, commands)
    }

    async fn observe_resume(
        owner: &SovereignCallbackSupervisor,
        value: serde_json::Value,
    ) -> io::Result<()> {
        let lease = owner.begin_execution(
            "thread/resume",
            Some(&serde_json::json!({"threadId":"thread"})),
        );
        owner
            .response("thread/resume", &Ok(Ok(value)), lease.as_ref())
            .await
    }

    #[tokio::test]
    async fn recovery_resume_terminal_status_reconciles_exact_turn_once() {
        for status in ["completed", "interrupted", "failed"] {
            let (_root, owner, _commands) = retained_turn_fixture().await;
            let response = resume_with_turns(serde_json::json!([{"id":"turn","status":status}]));
            observe_resume(&owner, response.clone()).await.unwrap();
            assert!(
                owner.inner.turns.lock().unwrap().active.is_empty(),
                "explicit {status} evidence did not settle the retained turn"
            );
            observe_resume(&owner, response.clone()).await.unwrap();
            let notification = ServerNotification::TurnCompleted(
                codex_app_server_protocol::TurnCompletedNotification {
                    thread_id: "thread".into(),
                    turn: codex_app_server_protocol::Turn {
                        id: "turn".into(),
                        root_turn_id: None,
                        items_view: codex_app_server_protocol::TurnItemsView::Full,
                        items: Vec::new(),
                        status: serde_json::from_value(serde_json::json!(status)).unwrap(),
                        error: None,
                        started_at: None,
                        completed_at: Some(0),
                        duration_ms: Some(1),
                    },
                },
            );
            owner.feed_notification(&notification);
            owner.feed_notification(&notification);
            assert!(
                owner.inner.turns.lock().unwrap().active.is_empty(),
                "duplicate terminal notifications revived the settled turn"
            );
            owner
                .inner
                .turns
                .lock()
                .unwrap()
                .started("thread".into(), "newer-turn".into());
            observe_resume(&owner, response).await.unwrap();
            owner.feed_notification(&notification);
            assert_eq!(
                owner
                    .inner
                    .turns
                    .lock()
                    .unwrap()
                    .active
                    .get("thread")
                    .map(String::as_str),
                Some("newer-turn"),
                "duplicate old response or notification cleared a replacement turn"
            );
            let ServerNotification::TurnCompleted(mut newer_notification) = notification else {
                unreachable!();
            };
            newer_notification.turn.id = "newer-turn".into();
            {
                let _turns = owner.inner.turns.lock().unwrap();
                owner.inner.epoch.fetch_add(1, Ordering::AcqRel);
            }
            owner.feed_notification(&ServerNotification::TurnCompleted(newer_notification));
            assert_eq!(
                owner
                    .inner
                    .turns
                    .lock()
                    .unwrap()
                    .active
                    .get("thread")
                    .map(String::as_str),
                Some("newer-turn"),
                "stale attachment's terminal notification cleared the current turn"
            );
            owner
                .inner
                .turns
                .lock()
                .unwrap()
                .completed("thread".into(), "newer-turn".into());
            owner.drain().await.unwrap();
        }
    }

    #[tokio::test]
    async fn recovery_resume_unknown_omitted_paginated_and_wrong_identity_remain_pending() {
        for turns in [
            serde_json::Value::Null,
            serde_json::json!([]),
            serde_json::json!([{"id":"turn","status":"inProgress"}]),
            serde_json::json!([{"id":"turn","status":"unknown"}]),
            serde_json::json!([{"id":"turn"}]),
            serde_json::json!([{"status":"completed"}]),
            serde_json::json!([{"id":"different-turn","status":"completed"}]),
        ] {
            let (_root, owner, _commands) = retained_turn_fixture().await;
            let mut response = resume_with_turns(turns.clone());
            response["turnsBackwardsCursor"] = serde_json::json!("more-history");
            observe_resume(&owner, response).await.unwrap();
            assert_eq!(
                owner
                    .inner
                    .turns
                    .lock()
                    .unwrap()
                    .active
                    .get("thread")
                    .map(String::as_str),
                Some("turn"),
                "unknown/omitted/other turn evidence cleared the obligation: {turns}"
            );
        }
        let (_root, owner, _commands) = retained_turn_fixture().await;
        owner
            .inner
            .threads
            .lock()
            .unwrap()
            .insert("other-admitted-thread".into());
        owner
            .inner
            .turns
            .lock()
            .unwrap()
            .started("other-admitted-thread".into(), "turn".into());
        let mut wrong_thread =
            resume_with_turns(serde_json::json!([{"id":"turn","status":"completed"}]));
        wrong_thread["thread"]["id"] = serde_json::json!("other-admitted-thread");
        let _ = observe_resume(&owner, wrong_thread).await;
        assert_eq!(
            owner.inner.turns.lock().unwrap().active.len(),
            2,
            "response thread must match the requested thread, even when both were admitted"
        );
        let mut omitted = scope_response();
        omitted["thread"].as_object_mut().unwrap().remove("turns");
        observe_resume(&owner, omitted).await.unwrap();
        assert_eq!(owner.inner.turns.lock().unwrap().active.len(), 2);
    }

    #[tokio::test]
    async fn recovery_resume_stale_scope_unbound_and_error_evidence_remain_pending() {
        let (_root, owner, _commands) = retained_turn_fixture().await;
        let terminal = resume_with_turns(serde_json::json!([{"id":"turn","status":"completed"}]));
        // A read response is not bound to the resume execution lease. Neither it
        // nor a failed interrupt is authority to infer terminal turn state.
        owner
            .response("thread/read", &Ok(Ok(terminal.clone())), None)
            .await
            .unwrap();
        owner
            .response(
                "thread/turns/list",
                &Ok(Ok(
                    serde_json::json!({"data":[{"id":"turn","status":"completed"}]}),
                )),
                None,
            )
            .await
            .unwrap();
        owner
            .response("thread/resume", &Ok(Ok(terminal.clone())), None)
            .await
            .unwrap();
        for method in ["thread/resume", "turn/interrupt"] {
            owner
                .response(
                    method,
                    &Err(io::Error::other("transport unavailable")),
                    None,
                )
                .await
                .unwrap();
            owner
                .response(
                    method,
                    &Ok(Err(codex_app_server_protocol::JSONRPCErrorError {
                        code: -32600,
                        message: "no active turn to interrupt".into(),
                        data: None,
                    })),
                    None,
                )
                .await
                .unwrap();
        }
        assert_eq!(
            owner
                .inner
                .turns
                .lock()
                .unwrap()
                .active
                .get("thread")
                .map(String::as_str),
            Some("turn")
        );
        let mut wrong_scope = terminal.clone();
        wrong_scope["cwd"] = serde_json::json!("/unrelated");
        assert!(observe_resume(&owner, wrong_scope).await.is_err());
        assert_eq!(owner.inner.turns.lock().unwrap().active.len(), 1);
        owner.inner.epoch.fetch_add(1, Ordering::AcqRel);
        assert!(observe_resume(&owner, terminal).await.is_err());
        assert_eq!(owner.inner.turns.lock().unwrap().active.len(), 1);
    }

    #[tokio::test]
    async fn recovery_bound_read_and_page_require_exact_terminal_evidence() {
        for method in ["thread/read", "thread/turns/list"] {
            let (_root, owner, _commands) = retained_turn_fixture().await;
            for turns in [
                serde_json::json!([]),
                serde_json::json!([{"id":"other","status":"completed"}]),
                serde_json::json!([{"id":"older","status":"inProgress"}]),
                serde_json::json!([{"id":"turn","status":"unknown"}]),
            ] {
                let value = if method == "thread/read" {
                    resume_with_turns(turns)
                } else {
                    serde_json::json!({"data":turns,"nextCursor":"more"})
                };
                let lease =
                    owner.begin_execution(method, Some(&serde_json::json!({"threadId":"thread"})));
                owner
                    .response(method, &Ok(Ok(value)), lease.as_ref())
                    .await
                    .unwrap();
                assert_eq!(
                    owner
                        .inner
                        .turns
                        .lock()
                        .unwrap()
                        .active
                        .get("thread")
                        .map(String::as_str),
                    Some("turn")
                );
            }
            if method == "thread/read" {
                owner
                    .inner
                    .threads
                    .lock()
                    .unwrap()
                    .insert("other-admitted".into());
                owner
                    .inner
                    .turns
                    .lock()
                    .unwrap()
                    .started("other-admitted".into(), "turn".into());
                let mut wrong_thread =
                    resume_with_turns(serde_json::json!([{"id":"turn","status":"completed"}]));
                wrong_thread["thread"]["id"] = serde_json::json!("other-admitted");
                let lease =
                    owner.begin_execution(method, Some(&serde_json::json!({"threadId":"thread"})));
                let _ = owner
                    .response(method, &Ok(Ok(wrong_thread)), lease.as_ref())
                    .await;
                drop(lease);
                assert_eq!(
                    owner.inner.turns.lock().unwrap().active.len(),
                    2,
                    "mismatched response cleared another admitted thread"
                );
                owner
                    .inner
                    .turns
                    .lock()
                    .unwrap()
                    .completed("other-admitted".into(), "turn".into());
            }
            let turns = serde_json::json!([{"id":"turn","status":"completed"}]);
            let value = if method == "thread/read" {
                resume_with_turns(turns)
            } else {
                serde_json::json!({"data":turns,"nextCursor":"more"})
            };
            // A page has no thread ID in its payload: only the original request's
            // binding establishes which admitted thread this evidence belongs to.
            let wrong = owner.begin_execution(
                method,
                Some(&serde_json::json!({"threadId":"other-thread"})),
            );
            let _ = owner
                .response(method, &Ok(Ok(value.clone())), wrong.as_ref())
                .await;
            drop(wrong);
            assert_eq!(
                owner
                    .inner
                    .turns
                    .lock()
                    .unwrap()
                    .active
                    .get("thread")
                    .map(String::as_str),
                Some("turn")
            );
            let lease =
                owner.begin_execution(method, Some(&serde_json::json!({"threadId":"thread"})));
            owner
                .response(method, &Ok(Ok(value.clone())), lease.as_ref())
                .await
                .unwrap();
            owner
                .response(method, &Ok(Ok(value)), lease.as_ref())
                .await
                .unwrap();
            drop(lease);
            assert!(
                owner.inner.turns.lock().unwrap().active.is_empty(),
                "bound {method} ignored the exact terminal turn"
            );
            owner.drain().await.unwrap();
        }
    }

    #[tokio::test]
    async fn recovery_resume_epoch_change_during_admit_preserves_turn() {
        let (_root, mut owner, _commands) = retained_turn_fixture().await;
        // Replace only the test owner's command sender so the admission receipt
        // can be held while a newer attachment invalidates this response.
        let (send, mut receive) = mpsc::unbounded_channel();
        Arc::get_mut(&mut owner.inner).unwrap().commands = send;
        let responding = owner.clone();
        let task = tokio::spawn(async move {
            observe_resume(
                &responding,
                resume_with_turns(serde_json::json!([{"id":"turn","status":"completed"}])),
            )
            .await
        });
        let Command::Admit(thread, done) =
            tokio::time::timeout(Duration::from_secs(3), receive.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("expected admission checkpoint");
        };
        assert_eq!(thread, "thread");
        {
            let _turns = owner.inner.turns.lock().unwrap();
            owner.inner.epoch.fetch_add(1, Ordering::AcqRel);
        }
        done.send(Ok(())).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap()
                .is_err(),
            "response admitted before reconnect mutated the new attachment"
        );
        assert_eq!(
            owner
                .inner
                .turns
                .lock()
                .unwrap()
                .active
                .get("thread")
                .map(String::as_str),
            Some("turn")
        );
        owner
            .inner
            .turns
            .lock()
            .unwrap()
            .completed("thread".into(), "turn".into());
    }

    #[tokio::test]
    async fn recovery_generic_interrupt_refusal_does_not_release_drain() {
        let (_root, owner, mut commands) = retained_turn_fixture().await;
        let draining_owner = owner.clone();
        let mut drain = tokio::spawn(async move { draining_owner.drain().await });
        let ClientCommand::Request {
            request,
            response_tx,
        } = tokio::time::timeout(Duration::from_secs(3), commands.recv())
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected interrupt");
        };
        assert!(matches!(*request, ClientRequest::TurnInterrupt { .. }));
        response_tx
            .send(Ok(Err(codex_app_server_protocol::JSONRPCErrorError {
                code: -32600,
                message: "no active turn to interrupt".into(),
                data: None,
            })))
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut drain)
                .await
                .is_err()
        );
        assert_eq!(
            owner
                .inner
                .turns
                .lock()
                .unwrap()
                .active
                .get("thread")
                .map(String::as_str),
            Some("turn")
        );
        // Fixture teardown only: do not treat the refusal as a terminal receipt.
        owner
            .inner
            .turns
            .lock()
            .unwrap()
            .completed("thread".into(), "turn".into());
        tokio::time::timeout(Duration::from_secs(3), drain)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn recovery_terminal_turn_does_not_acknowledge_pending_callback() {
        let (root, handle, mut commands) = fixture();
        let owner = SovereignCallbackSupervisor::start_inner(
            handle.clone(),
            root.path().to_owned(),
            "fixture".into(),
            false,
        )
        .unwrap();
        admit(&owner).await;
        let allow_ack = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gate = Arc::clone(&allow_ack);
        let server = tokio::spawn(async move {
            let mut acknowledged = false;
            while let Some(ClientCommand::Request {
                request,
                response_tx,
            }) = commands.recv().await
            {
                let result = match *request {
                    ClientRequest::DynamicToolCleanupRead { .. } => Ok(Ok(
                        serde_json::json!({"status":if acknowledged {"acknowledged"} else {"running"}}),
                    )),
                    ClientRequest::DynamicToolCleanupAck { params, .. } => {
                        assert!(params.response.cleanup_complete);
                        if gate.load(Ordering::Acquire) {
                            acknowledged = true;
                            Ok(Ok(serde_json::json!({"alreadyAcknowledged":false})))
                        } else {
                            Err(io::Error::other("cleanup acknowledgement withheld"))
                        }
                    }
                    ClientRequest::TurnInterrupt { .. } => {
                        Ok(Err(codex_app_server_protocol::JSONRPCErrorError {
                            code: -32600,
                            message: "no active turn to interrupt".into(),
                            data: None,
                        }))
                    }
                    _ => panic!("unexpected fixture RPC"),
                };
                let _ = response_tx.send(result);
            }
        });
        assert!(owner.feed_request(&ServerRequest::DynamicToolCall {
            request_id: RequestId::Integer(21),
            params: DynamicToolCallParams {
                cleanup_token: Some("pending-cleanup-capability".into()),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                call_id: "call".into(),
                namespace: Some("witnessops".into()),
                tool: "read_head".into(),
                arguments: serde_json::json!({})
            },
        }));
        until(|| {
            owner
                .status()
                .calls
                .first()
                .is_some_and(|call| call.reaped && call.state == CallbackState::AckPending)
        })
        .await;
        owner.disconnected();
        owner.reconnect("fixture", handle.clone()).await.unwrap();
        let attachment = slot();
        install(&attachment, owner.clone()).unwrap();
        let current = super::owner(&attachment).unwrap();
        observe_resume(
            &current,
            resume_with_turns(serde_json::json!([{"id":"turn","status":"completed"}])),
        )
        .await
        .unwrap();
        let draining_owner = current.clone();
        let mut drain = tokio::spawn(async move { draining_owner.drain().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut drain)
                .await
                .is_err(),
            "terminal turn must not manufacture callback cleanup acknowledgement"
        );
        assert_ne!(current.status().calls[0].state, CallbackState::Acknowledged);
        allow_ack.store(true, Ordering::Release);
        let result = tokio::time::timeout(Duration::from_secs(3), &mut drain).await;
        let reconciled = current.inner.turns.lock().unwrap().active.is_empty();
        // Always release the synthetic server after recording the actual result.
        current
            .inner
            .turns
            .lock()
            .unwrap()
            .completed("thread".into(), "turn".into());
        if result.is_err() {
            tokio::time::timeout(Duration::from_secs(3), drain)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        drop(current);
        drop(attachment);
        drop(owner);
        drop(handle);
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
        assert!(
            reconciled,
            "explicit terminal evidence did not clear the retained turn"
        );
        result.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn recovery_actual_server_lost_terminal_notification_resumes_and_drains()
    -> anyhow::Result<()> {
        actual_server_lost_terminal_notification(false).await
    }

    #[tokio::test]
    async fn recovery_actual_server_paginated_resume_waits_for_terminal_page() -> anyhow::Result<()>
    {
        actual_server_lost_terminal_notification(true).await
    }

    async fn actual_server_lost_terminal_notification(paginated: bool) -> anyhow::Result<()> {
        use crate::sovereign_callback_integration_tests::Fixture;
        use crate::sovereign_callback_integration_tests::lose_first_ack_receipt;
        use anyhow::Context;
        use codex_app_server_protocol::ThreadHistoryMode;
        use codex_app_server_protocol::ThreadResumeParams;
        let history_mode = if paginated {
            ThreadHistoryMode::Paginated
        } else {
            ThreadHistoryMode::Legacy
        };
        let mut actual = Fixture::start_persisted("supervisor-lost-terminal", history_mode)
            .await
            .context("starting persistent actual-server fixture")?;
        let (proxy, lost, proxy_task) = lose_first_ack_receipt(actual.client.request_handle());
        let retained = SovereignCallbackSupervisor::start_inner(
            proxy.clone(),
            actual.repo.path().to_owned(),
            "actual-in-process".into(),
            false,
        )?;
        retained
            .response(
                "thread/start",
                &Ok(Ok(serde_json::to_value(&actual.start_response)?)),
                None,
            )
            .await?;
        assert!(retained.feed_request(&actual.request));
        until(|| lost.load(Ordering::SeqCst)).await;
        retained.disconnected();
        // Consume the genuine notification without delivering it to the owner:
        // this is precisely the transport-loss fault under test.
        actual
            .completed_turn()
            .await
            .context("waiting for the genuine missed terminal notification")?;
        assert_eq!(
            retained
                .inner
                .turns
                .lock()
                .unwrap()
                .active
                .get(&actual.identity.thread_id),
            Some(&actual.identity.turn_id)
        );
        retained
            .reconnect("actual-in-process", actual.client.request_handle())
            .await?;
        let attachment = slot();
        install(&attachment, retained.clone())?;
        let current = super::owner(&attachment).unwrap();
        let params = serde_json::json!({"threadId":actual.identity.thread_id});
        let lease = current.begin_execution("thread/resume", Some(&params));
        let response = actual
            .client
            .request(ClientRequest::ThreadResume {
                request_id: RequestId::String("recovery-terminal-resume".into()),
                params: ThreadResumeParams {
                    thread_id: actual.identity.thread_id.clone(),
                    exclude_turns: paginated,
                    ..Default::default()
                },
            })
            .await;
        let value = response
            .as_ref()
            .map_err(|error| anyhow::anyhow!("actual resume transport failed: {error}"))?
            .as_ref()
            .map_err(|error| anyhow::anyhow!("actual resume rejected: {error:?}"))?;
        if !paginated {
            assert!(
                value["thread"]["turns"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|turn| turn["id"] == actual.identity.turn_id
                        && matches!(
                            turn["status"].as_str(),
                            Some("completed" | "interrupted" | "failed")
                        )),
                "actual resume omitted the exact terminal turn"
            );
        }
        current
            .response("thread/resume", &response, lease.as_ref())
            .await?;
        drop(lease);
        if paginated {
            assert_eq!(
                current
                    .inner
                    .turns
                    .lock()
                    .unwrap()
                    .active
                    .get(&actual.identity.thread_id),
                Some(&actual.identity.turn_id),
                "metadata-only resume must retain unknown turn state"
            );
            let lease = current.begin_execution("thread/turns/list", Some(&params));
            let page = actual
                .client
                .request(ClientRequest::ThreadTurnsList {
                    request_id: RequestId::String("recovery-terminal-page".into()),
                    params: codex_app_server_protocol::ThreadTurnsListParams {
                        thread_id: actual.identity.thread_id.clone(),
                        cursor: None,
                        limit: Some(10),
                        sort_direction: None,
                        items_view: None,
                    },
                })
                .await;
            let value = page
                .as_ref()
                .map_err(|error| anyhow::anyhow!("actual terminal page transport failed: {error}"))?
                .as_ref()
                .map_err(|error| anyhow::anyhow!("actual terminal page rejected: {error:?}"))?;
            assert!(
                value["data"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|turn| turn["id"] == actual.identity.turn_id
                        && matches!(
                            turn["status"].as_str(),
                            Some("completed" | "interrupted" | "failed")
                        )),
                "actual page omitted the exact terminal turn"
            );
            current
                .response("thread/turns/list", &page, lease.as_ref())
                .await?;
        }
        until(|| {
            current
                .status()
                .calls
                .first()
                .is_some_and(|call| call.state == CallbackState::Acknowledged)
        })
        .await;
        assert_eq!(current.status().calls[0].execution_count, 1);
        assert!(current.status().calls[0].reaped);
        let result = tokio::time::timeout(Duration::from_secs(3), current.drain()).await;
        // Preserve the observed result, then finish fixture teardown even on RED.
        current.inner.turns.lock().unwrap().completed(
            actual.identity.thread_id.clone(),
            actual.identity.turn_id.clone(),
        );
        current.drain().await?;
        drop(current);
        drop(attachment);
        drop(retained);
        drop(proxy);
        tokio::time::timeout(Duration::from_secs(3), proxy_task).await??;
        actual.shutdown().await?;
        assert!(
            result.is_ok(),
            "verified same-generation resume failed to reconcile a missed terminal notification; drain remained HOLD"
        );
        result.unwrap()?;
        Ok(())
    }
}
