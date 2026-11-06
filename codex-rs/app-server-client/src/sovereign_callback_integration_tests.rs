//! Production adapter + in-process app-server tests. The model stream is canned
//! local SSE; no inference, real provider, credentials or installed runtime is used.
#![cfg(target_os = "linux")]

use super::*;
use crate::sovereign_callback::CallbackState;
use crate::sovereign_callback::SovereignCallbackClient;
use anyhow::Context;
use anyhow::Result;
use app_test_support::create_final_assistant_message_sse_response;
use app_test_support::create_mock_responses_server_sequence_unchecked;
use app_test_support::write_models_cache_with_models;
use codex_app_server_protocol::DynamicToolCallResponse;
use codex_app_server_protocol::DynamicToolCallStatus;
use codex_app_server_protocol::DynamicToolCleanupAckParams;
use codex_app_server_protocol::DynamicToolCleanupAckResponse;
use codex_app_server_protocol::DynamicToolCleanupIdentity;
use codex_app_server_protocol::DynamicToolCleanupReadParams;
use codex_app_server_protocol::DynamicToolCleanupReadResponse;
use codex_app_server_protocol::DynamicToolCleanupStatus;
use codex_app_server_protocol::ThreadItemPayload;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnInterruptParams;
use codex_app_server_protocol::TurnInterruptResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use core_test_support::load_default_config_for_test;
use core_test_support::responses;
use futures::FutureExt;
use serde_json::json;
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tempfile::TempDir;
use tokio::sync::Notify;

const GENERATION: &str = "isolated-in-process-server-generation";
const HEAD: &str = "0123456789abcdef0123456789abcdef01234567";
// The isolated runner mounts its own empty fixture at this required policy path.
// The operator's checkout is never mounted. The adapter separately owns `repo`.
const OWNER_CWD: &str = "/home/karol/WitnessOps/WitnessOps/repos/witnessops-codex-ops";
const DEADLINE: Duration = Duration::from_secs(15);

struct Fixture {
    _codex_home: TempDir,
    repo: TempDir,
    server: wiremock::MockServer,
    client: AppServerClient,
    request: ServerRequest,
    identity: DynamicToolCleanupIdentity,
    model_call_id: String,
}

impl Fixture {
    async fn start(call_id: &str) -> Result<Self> {
        let server = create_mock_responses_server_sequence_unchecked(vec![
            responses::sse(vec![
                responses::ev_response_created("callback-model-1"),
                responses::ev_custom_tool_call(
                    call_id,
                    "exec",
                    "// @exec: {\"yield_time_ms\":10000}\ntext(await tools.witnessops__read_head({}));",
                ),
                responses::ev_completed("callback-model-1"),
            ]),
            create_final_assistant_message_sse_response("Synthetic callback finished")?,
        ])
        .await;
        let codex_home = TempDir::new()?;
        let repo = TempDir::new()?;
        // A minimal local Git fixture is enough for rev-parse. No git worktree
        // command, commit, credential helper, hook or network access is involved.
        let git = repo.path().join(".git");
        std::fs::create_dir_all(git.join("refs/heads"))?;
        std::fs::create_dir_all(git.join("objects"))?;
        std::fs::write(git.join("HEAD"), "ref: refs/heads/main\n")?;
        std::fs::write(git.join("refs/heads/main"), format!("{HEAD}\n"))?;
        std::fs::write(
            git.join("config"),
            "[core]\nrepositoryformatversion = 0\nbare = false\n",
        )?;
        std::fs::write(
            codex_home.path().join("config.toml"),
            format!(
                r#"
model = "gpt-5.6-terra"
model_provider = "mock_provider"
default_permissions = "wops-codex-ops-read"
approval_policy = "never"
cli_auth_credentials_store = "file"

[permissions.wops-codex-ops-read]
extends = ":read-only"

[model_providers.mock_provider]
name = "Isolated canned callback fixture"
base_url = "{}/v1"
wire_api = "responses"
requires_openai_auth = false
supports_websockets = false
request_max_retries = 0
stream_max_retries = 0

[analytics]
enabled = false

[feedback]
enabled = false
"#,
                server.uri()
            ),
        )?;
        let config = load_default_config_for_test(&codex_home).await;
        let mut model =
            codex_core::test_support::construct_model_info_offline("gpt-5.6-terra", &config);
        model.tool_mode = Some(codex_protocol::openai_models::ToolMode::CodeModeOnly);
        write_models_cache_with_models(codex_home.path(), vec![model]).await?;
        let state_db = codex_core::init_state_db(&config)
            .await
            .context("fixture state DB unavailable")?;
        let mut client = AppServerClient::InProcess(
            InProcessAppServerClient::start_with_owner_mode(
                InProcessClientStartArgs {
                    required_human_approval: false,
                    arg0_paths: Arg0DispatchPaths::default(),
                    config: Arc::new(config),
                    cli_overrides: Vec::new(),
                    loader_overrides: LoaderOverrides::without_managed_config_for_tests(),
                    strict_config: false,
                    cloud_config_bundle: CloudConfigBundleLoader::default(),
                    embedded_network_policy: Default::default(),
                    feedback: CodexFeedback::new(),
                    log_db: None,
                    state_db: Some(state_db),
                    environment_manager: Arc::new(EnvironmentManager::default_for_tests()),
                    config_warnings: Vec::new(),
                    session_source: SessionSource::Exec,
                    enable_codex_api_key_env: false,
                    client_name: "isolated-sovereign-callback-test".into(),
                    client_version: "0.0.0-test".into(),
                    experimental_api: true,
                    mcp_server_openai_form_elicitation: false,
                    opt_out_notification_methods: Vec::new(),
                    channel_capacity: DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
                },
                codex_app_server::DynamicToolOwnerMode::SovereignReadHead,
            )
            .await?,
        );
        let thread: ThreadStartResponse = client
            .request_typed(ClientRequest::ThreadStart {
                request_id: RequestId::Integer(1),
                params: ThreadStartParams {
                    cwd: Some(OWNER_CWD.into()),
                    model: Some("gpt-5.6-terra".into()),
                    permissions: Some("wops-codex-ops-read".into()),
                    ephemeral: Some(true),
                    ..Default::default()
                },
            })
            .await?;
        let turn: TurnStartResponse = client
            .request_typed(ClientRequest::TurnStart {
                request_id: RequestId::Integer(2),
                params: TurnStartParams {
                    thread_id: thread.thread.id.clone(),
                    input: vec![UserInput::Text {
                        text: "Consume the fixed local callback fixture".into(),
                        text_elements: Vec::new(),
                    }],
                    ..Default::default()
                },
            })
            .await?;
        let request = timeout(DEADLINE, async {
            loop {
                match client.next_event().await.context("event stream closed")? {
                    AppServerEvent::ServerRequest(request)
                        if matches!(request.as_ref(), ServerRequest::DynamicToolCall { .. }) =>
                    {
                        break Ok::<_, anyhow::Error>(*request);
                    }
                    AppServerEvent::Disconnected { .. } => anyhow::bail!("unexpected disconnect"),
                    _ => {}
                }
            }
        })
        .await??;
        let ServerRequest::DynamicToolCall { request_id, params } = &request else {
            unreachable!();
        };
        assert_eq!(params.thread_id, thread.thread.id);
        assert_eq!(params.turn_id, turn.turn.id);
        assert!(!params.call_id.is_empty());
        assert_eq!(params.namespace.as_deref(), Some("witnessops"));
        assert_eq!(params.tool, "read_head");
        assert_eq!(params.arguments, json!({}));
        let identity = DynamicToolCleanupIdentity {
            thread_id: params.thread_id.clone(),
            turn_id: params.turn_id.clone(),
            call_id: params.call_id.clone(),
            request_id: request_id.clone(),
            capability: params
                .cleanup_token
                .clone()
                .context("cleanup capability missing")?,
        };
        Ok(Self {
            _codex_home: codex_home,
            repo,
            server,
            client,
            request,
            identity,
            model_call_id: call_id.into(),
        })
    }

    fn owner(&self, handle: AppServerRequestHandle) -> Result<SovereignCallbackClient> {
        Ok(SovereignCallbackClient::new(
            handle,
            GENERATION.into(),
            self.repo.path().to_path_buf(),
            [self.identity.thread_id.clone()],
        )?)
    }

    async fn status(&self) -> Result<DynamicToolCleanupStatus> {
        let response: DynamicToolCleanupReadResponse = self
            .client
            .request_typed(ClientRequest::DynamicToolCleanupRead {
                request_id: RequestId::String("integration-cleanup-status".into()),
                params: DynamicToolCleanupReadParams {
                    identity: self.identity.clone(),
                },
            })
            .await?;
        Ok(response.status)
    }

    fn interrupt(
        &self,
    ) -> tokio::task::JoinHandle<Result<TurnInterruptResponse, TypedRequestError>> {
        let handle = self.client.request_handle();
        let params = TurnInterruptParams {
            thread_id: self.identity.thread_id.clone(),
            turn_id: self.identity.turn_id.clone(),
        };
        tokio::spawn(async move {
            handle
                .request_typed(ClientRequest::TurnInterrupt {
                    request_id: RequestId::Integer(3),
                    params,
                })
                .await
        })
    }

    async fn cancelled_item(&mut self) -> Result<ServerNotification> {
        timeout(DEADLINE, async {
            loop {
                if let AppServerEvent::ServerNotification(notification) = self.client.next_event().await.context("event stream closed")? {
                    if let ServerNotification::ItemCompleted(item) = notification.as_ref()
                        && item.thread_id == self.identity.thread_id
                        && item.turn_id == self.identity.turn_id
                        && matches!(&item.item.payload, ThreadItemPayload::DynamicToolCall { id, status: DynamicToolCallStatus::Failed, .. } if id == &self.identity.call_id)
                    { return Ok(*notification); }
                    assert!(!matches!(notification.as_ref(), ServerNotification::TurnCompleted(_)), "turn completed before callback cleanup acknowledgement");
                }
            }
        }).await?
    }

    async fn completed_turn(&mut self) -> Result<()> {
        timeout(DEADLINE, async {
            loop {
                match self
                    .client
                    .next_event()
                    .await
                    .context("event stream closed")?
                {
                    AppServerEvent::ServerNotification(notification) => {
                        if let ServerNotification::TurnCompleted(done) = notification.as_ref()
                            && done.thread_id == self.identity.thread_id
                            && done.turn.id == self.identity.turn_id
                        {
                            return Ok(());
                        }
                    }
                    AppServerEvent::ServerRequest(request)
                        if matches!(request.as_ref(), ServerRequest::DynamicToolCall { .. }) =>
                    {
                        anyhow::bail!("server replayed callback execution");
                    }
                    _ => {}
                }
            }
        })
        .await?
    }

    async fn shutdown(self) -> Result<()> {
        self.client.shutdown().await?;
        Ok(())
    }
}

async fn wait_owner_state(
    owner: &mut SovereignCallbackClient,
    expected: CallbackState,
) -> Result<()> {
    timeout(DEADLINE, async {
        loop {
            owner.poll().await;
            if owner.snapshots().iter().any(|item| item.state == expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("callback owner did not reach expected state")
}

async fn wait_for_reaping(owner: &SovereignCallbackClient) -> Result<u32> {
    let pid = timeout(DEADLINE, async {
        loop {
            if let Some(pid) = owner
                .snapshots()
                .into_iter()
                .find_map(|item| item.reaped.then_some(item.process_id).flatten())
            {
                return pid;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    let error = std::fs::metadata(Path::new("/proc").join(pid.to_string()))
        .expect_err("git process still present after reaping");
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    Ok(pid)
}

#[tokio::test]
async fn sovereign_callback_client_actual_server_read_head_completes_once() -> Result<()> {
    let mut fixture = Fixture::start("actual-read-head").await?;
    let mut owner = fixture.owner(fixture.client.request_handle())?;
    assert!(owner.dispatch(GENERATION, &fixture.request).await?);
    wait_owner_state(&mut owner, CallbackState::Acknowledged).await?;
    wait_for_reaping(&owner).await?;
    assert_eq!(owner.snapshots()[0].execution_count, 1);
    assert_eq!(
        fixture.status().await?,
        DynamicToolCleanupStatus::Acknowledged
    );
    assert!(owner.dispatch(GENERATION, &fixture.request).await.is_err());
    fixture.completed_turn().await?;
    let requests = fixture
        .server
        .received_requests()
        .await
        .context("fixture did not record requests")?;
    let outputs: Vec<_> = requests
        .iter()
        .filter_map(|request| serde_json::from_slice::<serde_json::Value>(&request.body).ok())
        .flat_map(|body| body["input"].as_array().cloned().unwrap_or_default())
        .filter(|item| {
            item["type"] == "custom_tool_call_output" && item["call_id"] == fixture.model_call_id
        })
        .collect();
    assert_eq!(outputs.len(), 1);
    assert!(outputs[0]["output"].to_string().contains(HEAD));
    fixture.shutdown().await
}

#[tokio::test]
async fn sovereign_callback_client_actual_server_waits_for_cleanup_completion() -> Result<()> {
    let mut fixture = Fixture::start("actual-cancellation").await?;
    let head = fixture.repo.path().join(".git/HEAD");
    std::fs::remove_file(&head)?;
    anyhow::ensure!(
        std::process::Command::new("/usr/bin/mkfifo")
            .arg(&head)
            .env_clear()
            .status()?
            .success(),
        "could not create owned Git fixture FIFO"
    );
    let mut owner = fixture.owner(fixture.client.request_handle())?;
    let complete = Arc::new(Notify::new());
    owner.set_test_completion_gate(Arc::clone(&complete));
    // Always perform owner cleanup before propagating any assertion failure.
    // The only intentionally blocked process is fixed git on this owned FIFO.
    let result = AssertUnwindSafe(async {
        assert!(owner.dispatch(GENERATION, &fixture.request).await?);
        let pid = timeout(DEADLINE, async {
            loop {
                if let Some(pid) = owner
                    .snapshots()
                    .into_iter()
                    .find_map(|item| (!item.reaped).then_some(item.process_id).flatten())
                {
                    return pid;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        // Independently observe a live process before asking the real server
        // to interrupt. Zombie/dead processes are not accepted as the canary.
        let before = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
        let state = before.rsplit_once(") ").context("proc stat format")?.1;
        assert!(!state.starts_with('Z') && !state.starts_with('X'));
        let interrupt = fixture.interrupt();
        let cancelled = fixture.cancelled_item().await?;
        owner.observe_notification(GENERATION, &cancelled)?;
        assert_eq!(wait_for_reaping(&owner).await?, pid);
        tokio::time::sleep(Duration::from_millis(500)).await;
        owner.poll().await;
        assert!(!interrupt.is_finished());
        assert_eq!(
            fixture.status().await?,
            DynamicToolCleanupStatus::Cancelling
        );
        assert!(
            !owner
                .snapshots()
                .iter()
                .any(|item| item.state == CallbackState::Acknowledged)
        );
        complete.notify_one();
        wait_owner_state(&mut owner, CallbackState::Acknowledged).await?;
        timeout(DEADLINE, interrupt).await???;
        fixture.completed_turn().await?;
        assert_eq!(owner.snapshots()[0].execution_count, 1);
        Ok::<_, anyhow::Error>(())
    })
    .catch_unwind()
    .await;
    owner.disconnected();
    complete.notify_one();
    let reaping = if owner
        .snapshots()
        .iter()
        .any(|item| item.process_id.is_some())
    {
        wait_for_reaping(&owner).await.map(|_| ())
    } else {
        Ok(())
    };
    owner.reconnect(GENERATION, fixture.client.request_handle())?;
    let acknowledgement = if owner.snapshots().is_empty() {
        Ok(())
    } else {
        wait_owner_state(&mut owner, CallbackState::Acknowledged).await
    };
    let shutdown = fixture.shutdown().await;
    reaping?;
    acknowledgement?;
    shutdown?;
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[tokio::test]
async fn sovereign_callback_cleanup_actual_server_accepts_explicit_inline_receipt() -> Result<()> {
    let mut fixture = Fixture::start("inline-cleanup-receipt").await?;
    fixture
        .client
        .resolve_server_request(
            fixture.identity.request_id.clone(),
            json!({"contentItems":[],"success":false,"cleanupComplete":"malformed"}),
        )
        .await?;
    assert_eq!(fixture.status().await?, DynamicToolCleanupStatus::Running);

    // This fixture starts no external executor. Its in-memory failure result is complete.
    let response = DynamicToolCallResponse {
        content_items: Vec::new(),
        success: false,
        cleanup_complete: true,
    };
    fixture
        .client
        .resolve_server_request(
            fixture.identity.request_id.clone(),
            serde_json::to_value(&response)?,
        )
        .await?;
    timeout(DEADLINE, async {
        loop {
            if fixture.status().await? == DynamicToolCleanupStatus::Acknowledged {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    let repeated: DynamicToolCleanupAckResponse = fixture
        .client
        .request_typed(ClientRequest::DynamicToolCleanupAck {
            request_id: RequestId::Integer(30),
            params: DynamicToolCleanupAckParams {
                identity: fixture.identity.clone(),
                response,
            },
        })
        .await?;
    assert!(repeated.already_acknowledged);
    fixture.completed_turn().await?;
    fixture.shutdown().await
}

#[tokio::test]
async fn sovereign_callback_cleanup_actual_server_recovers_malformed_reply() -> Result<()> {
    let mut fixture = Fixture::start("malformed-recovery").await?;
    let interrupt = fixture.interrupt();
    fixture.cancelled_item().await?;
    let mut wrong = fixture.identity.clone();
    wrong.capability = "wrong-capability".into();
    let response = DynamicToolCallResponse {
        content_items: Vec::new(),
        success: false,
        cleanup_complete: true,
    };
    let bad = fixture
        .client
        .request_typed::<DynamicToolCleanupAckResponse>(ClientRequest::DynamicToolCleanupAck {
            request_id: RequestId::Integer(20),
            params: DynamicToolCleanupAckParams {
                identity: wrong,
                response: response.clone(),
            },
        })
        .await;
    assert!(bad.is_err());
    fixture
        .client
        .resolve_server_request(
            fixture.identity.request_id.clone(),
            json!({"contentItems":[],"success":false,"cleanupComplete":"malformed"}),
        )
        .await?;
    fixture
        .client
        .resolve_server_request(
            fixture.identity.request_id.clone(),
            json!({"contentItems":[],"success":false,"cleanupComplete":false}),
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!interrupt.is_finished());
    assert_eq!(
        fixture.status().await?,
        DynamicToolCleanupStatus::Cancelling
    );
    for (id, already) in [(21, false), (22, true)] {
        let accepted: DynamicToolCleanupAckResponse = fixture
            .client
            .request_typed(ClientRequest::DynamicToolCleanupAck {
                request_id: RequestId::Integer(id),
                params: DynamicToolCleanupAckParams {
                    identity: fixture.identity.clone(),
                    response: response.clone(),
                },
            })
            .await?;
        assert_eq!(accepted.already_acknowledged, already);
    }
    timeout(DEADLINE, interrupt).await???;
    fixture.completed_turn().await?;
    fixture.shutdown().await
}

/// The real server consumes the first acknowledgement; the proxy then loses the
/// receipt at the client transport boundary. Only that fault is synthetic.
fn lose_first_ack_receipt(
    real: AppServerRequestHandle,
) -> (
    AppServerRequestHandle,
    Arc<AtomicBool>,
    tokio::task::JoinHandle<()>,
) {
    let (command_tx, mut command_rx) = mpsc::channel(8);
    let lost = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&lost);
    let task = tokio::spawn(async move {
        while let Some(command) = command_rx.recv().await {
            let ClientCommand::Request {
                request,
                response_tx,
            } = command
            else {
                panic!("unexpected transport command");
            };
            let acknowledgement = matches!(
                request.as_ref(),
                ClientRequest::DynamicToolCleanupAck { .. }
            );
            let result = real.request(*request).await;
            if acknowledgement
                && result.as_ref().is_ok_and(|inner| inner.is_ok())
                && !observed.swap(true, Ordering::SeqCst)
            {
                let _ = response_tx.send(Err(IoError::new(
                    ErrorKind::ConnectionReset,
                    "injected lost cleanup receipt",
                )));
            } else {
                let _ = response_tx.send(result);
            }
        }
    });
    (
        AppServerRequestHandle::InProcess(InProcessAppServerRequestHandle { command_tx }),
        lost,
        task,
    )
}

#[tokio::test]
async fn sovereign_callback_client_actual_server_lost_receipt_retries_cleanup_only() -> Result<()> {
    let mut fixture = Fixture::start("lost-cleanup-receipt").await?;
    let (proxy, lost, proxy_task) = lose_first_ack_receipt(fixture.client.request_handle());
    let mut owner = fixture.owner(proxy.clone())?;
    assert!(owner.dispatch(GENERATION, &fixture.request).await?);
    wait_owner_state(&mut owner, CallbackState::AckPending).await?;
    assert!(lost.load(Ordering::SeqCst));
    wait_for_reaping(&owner).await?;
    assert_eq!(
        fixture.status().await?,
        DynamicToolCleanupStatus::Acknowledged
    );
    owner.disconnected();
    owner.reconnect(GENERATION, fixture.client.request_handle())?;
    drop(proxy);
    timeout(DEADLINE, proxy_task).await??;
    wait_owner_state(&mut owner, CallbackState::Acknowledged).await?;
    assert_eq!(owner.snapshots()[0].execution_count, 1);
    assert!(owner.dispatch(GENERATION, &fixture.request).await.is_err());
    assert!(
        owner
            .reconnect(
                "replacement-server-generation",
                fixture.client.request_handle()
            )
            .is_err()
    );
    fixture.completed_turn().await?;
    assert_eq!(
        fixture
            .server
            .received_requests()
            .await
            .context("fixture requests missing")?
            .len(),
        2
    );
    fixture.shutdown().await
}

#[tokio::test]
async fn sovereign_callback_client_replacement_server_cannot_accept_old_cleanup() -> Result<()> {
    let mut original = Fixture::start("original-instance-call").await?;
    let mut replacement = Fixture::start("replacement-instance-call").await?;
    let mut owner = original.owner(original.client.request_handle())?;
    let complete = Arc::new(Notify::new());
    owner.set_test_completion_gate(Arc::clone(&complete));
    assert!(owner.dispatch(GENERATION, &original.request).await?);
    wait_for_reaping(&owner).await?;
    owner.disconnected();
    // Deliberately stale caller generation cannot bypass the server's original
    // capability binding, even when a new server is reachable through a handle.
    owner.reconnect(GENERATION, replacement.client.request_handle())?;
    complete.notify_one();
    wait_owner_state(&mut owner, CallbackState::Hold).await?;
    assert_eq!(owner.snapshots()[0].execution_count, 1);
    assert_eq!(original.status().await?, DynamicToolCleanupStatus::Running);
    assert_eq!(
        replacement.status().await?,
        DynamicToolCleanupStatus::Running
    );
    owner.reconnect(GENERATION, original.client.request_handle())?;
    wait_owner_state(&mut owner, CallbackState::Acknowledged).await?;
    original.completed_turn().await?;
    assert_eq!(
        replacement.status().await?,
        DynamicToolCleanupStatus::Running
    );
    // The replacement callback was never dispatched to an executor, so there
    // are no effects to clean up. Resolve only its own exact identity.
    let response: DynamicToolCleanupAckResponse = replacement
        .client
        .request_typed(ClientRequest::DynamicToolCleanupAck {
            request_id: RequestId::Integer(80),
            params: DynamicToolCleanupAckParams {
                identity: replacement.identity.clone(),
                response: DynamicToolCallResponse {
                    content_items: Vec::new(),
                    success: false,
                    cleanup_complete: true,
                },
            },
        })
        .await?;
    assert!(!response.already_acknowledged);
    replacement.completed_turn().await?;
    original.shutdown().await?;
    replacement.shutdown().await
}
