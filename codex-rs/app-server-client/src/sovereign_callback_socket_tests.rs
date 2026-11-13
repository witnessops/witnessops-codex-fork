//! Actual candidate app-server + remote WebSocket transport regression.
//! Run in the isolated test namespace with a private owner-cwd mount, no external
//! networking, and WOPS_TEST_APP_SERVER_BIN naming the freshly built candidate.
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
use codex_protocol::openai_models::ToolMode;
use core_test_support::load_default_config_for_test;
use core_test_support::responses;
use futures::FutureExt;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::process::Stdio;
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::process::Child;
use tokio::process::Command;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

const OWNER_CWD: &str = "/home/karol/WitnessOps/WitnessOps/repos/witnessops-codex-ops";
const DEADLINE: Duration = Duration::from_secs(20);

/// Byte-transparent TCP forwarding preserves the real client/server WebSocket
/// handshake and RPC routing. Cutting it drops both actual transport sockets.
struct CutConnection {
    endpoint: String,
    cut: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<()>>,
}

impl CutConnection {
    async fn start(server: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let (cut, mut requested) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut downstream, _) = tokio::select! {
                accepted = listener.accept() => accepted?,
                _ = &mut requested => return Ok(()),
            };
            let mut upstream = TcpStream::connect(server).await?;
            tokio::select! {
                _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream) => {},
                _ = &mut requested => {},
            }
            // Dropping both streams produces a genuine lost connection, with no
            // invented JSON-RPC disconnect or synthetic cleanup response.
            Ok(())
        });
        Ok(Self {
            endpoint,
            cut: Some(cut),
            task,
        })
    }

    async fn stop(mut self) -> Result<()> {
        if let Some(cut) = self.cut.take() {
            let _ = cut.send(());
        }
        timeout(Duration::from_secs(3), self.task).await???;
        Ok(())
    }
}

struct SocketFixture {
    _home: TempDir,
    _codex_home: TempDir,
    repo: TempDir,
    _provider: wiremock::MockServer,
    process: Child,
    stderr_drain: JoinHandle<()>,
    address: SocketAddr,
    generation: String,
    tunnel: Option<CutConnection>,
    client: Option<AppServerClient>,
    owner: Option<SovereignCallbackClient>,
    complete: Arc<Notify>,
}

impl SocketFixture {
    async fn start() -> Result<Self> {
        anyhow::ensure!(
            std::env::var("WOPS_TEST_OWNER_CWD_ISOLATED").as_deref() == Ok("1"),
            "socket regression requires WOPS_TEST_OWNER_CWD_ISOLATED=1 and the private owner-cwd mount"
        );
        let program = PathBuf::from(std::env::var_os("WOPS_TEST_APP_SERVER_BIN").context(
            "WOPS_TEST_APP_SERVER_BIN must name the freshly built candidate app-server binary",
        )?);
        anyhow::ensure!(
            program.is_absolute() && program.is_file(),
            "candidate app-server executable is missing or not absolute"
        );
        anyhow::ensure!(
            program
                .parent()
                .context("binary parent")?
                .join("codex-code-mode-host")
                .is_file(),
            "candidate code-mode-host sibling is required"
        );
        let provider = create_mock_responses_server_sequence_unchecked(vec![
            responses::sse(vec![
                responses::ev_response_created("socket-callback-model"),
                responses::ev_custom_tool_call("socket-model-call", "exec", "// @exec: {\"yield_time_ms\":10000}\ntext(await tools.witnessops__read_head({}));"),
                responses::ev_completed("socket-callback-model"),
            ]),
            create_final_assistant_message_sse_response("Synthetic socket callback finished")?,
        ]).await;
        let home = TempDir::new()?;
        let codex_home = TempDir::new()?;
        let repo = TempDir::new()?;
        let git = repo.path().join(".git");
        std::fs::create_dir_all(git.join("refs/heads"))?;
        std::fs::create_dir_all(git.join("objects"))?;
        std::fs::write(git.join("HEAD"), "ref: refs/heads/main\n")?;
        std::fs::write(
            git.join("refs/heads/main"),
            "0123456789abcdef0123456789abcdef01234567\n",
        )?;
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
thread_unload_delay_secs = 1
[features]
plugins = false
plugin_discovery_prefetch = false
apps = false
[permissions.wops-codex-ops-read]
extends = ":read-only"
[model_providers.mock_provider]
name = "Isolated socket fixture"
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
                provider.uri()
            ),
        )?;
        let config = load_default_config_for_test(&codex_home).await;
        let mut model =
            codex_core::test_support::construct_model_info_offline("gpt-5.6-terra", &config);
        model.tool_mode = Some(ToolMode::CodeModeOnly);
        write_models_cache_with_models(codex_home.path(), vec![model]).await?;
        let mut process = Command::new(&program)
            .args([
                "--listen",
                "ws://127.0.0.1:0",
                "--session-source",
                "exec",
                "--sovereign-read-head",
            ])
            .current_dir(repo.path())
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", home.path())
            .env("XDG_CONFIG_HOME", home.path().join("config"))
            .env("XDG_DATA_HOME", home.path().join("data"))
            .env("XDG_CACHE_HOME", home.path().join("cache"))
            .env("CODEX_HOME", codex_home.path())
            .env("CODEX_APP_SERVER_DISABLE_MANAGED_CONFIG", "1")
            .env("CODEX_INTERNAL_APP_SERVER_REMOTE_CONTROL_DISABLED", "1")
            .env("RUST_LOG", "warn")
            .env("LANG", "C")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("spawn candidate app-server")?;
        let generation = format!(
            "isolated-socket-child-{}",
            process.id().context("candidate PID")?
        );
        let mut stderr = BufReader::new(process.stderr.take().context("child stderr")?).lines();
        let bound = timeout(DEADLINE, async {
            while let Some(line) = stderr.next_line().await? {
                // Parse only a loopback address; never print server messages or capabilities.
                if let Some(address) = line
                    .split_whitespace()
                    .find_map(|word| word.strip_prefix("ws://"))
                    .and_then(|value| value.parse::<SocketAddr>().ok())
                {
                    anyhow::ensure!(
                        address.ip().is_loopback(),
                        "fixture listener is not loopback"
                    );
                    return Ok::<_, anyhow::Error>(address);
                }
            }
            anyhow::bail!("candidate exited before reporting its loopback address")
        })
        .await;
        let address = match bound {
            Ok(Ok(address)) => address,
            failure => {
                let _ = process.start_kill();
                let _ = timeout(Duration::from_secs(5), process.wait()).await;
                return Err(match failure {
                    Ok(Err(error)) => error,
                    Err(error) => error.into(),
                    _ => unreachable!(),
                });
            }
        };
        let stderr_drain =
            tokio::spawn(async move { while matches!(stderr.next_line().await, Ok(Some(_))) {} });
        Ok(Self {
            _home: home,
            _codex_home: codex_home,
            repo,
            _provider: provider,
            process,
            stderr_drain,
            address,
            generation,
            tunnel: None,
            client: None,
            owner: None,
            complete: Arc::new(Notify::new()),
        })
    }

    async fn connect(endpoint: String) -> Result<AppServerClient> {
        Ok(AppServerClient::Remote(
            RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
                required_human_approval: false,
                endpoint: RemoteAppServerEndpoint::WebSocket {
                    websocket_url: endpoint,
                    auth_token: None,
                },
                client_name: "isolated-sovereign-socket-test".into(),
                client_version: "0.0.0-test".into(),
                experimental_api: true,
                mcp_server_openai_form_elicitation: false,
                opt_out_notification_methods: Vec::new(),
                channel_capacity: 128,
            })
            .await?,
        ))
    }

    async fn exercise(&mut self) -> Result<()> {
        let tunnel = CutConnection::start(self.address).await?;
        self.client = Some(Self::connect(tunnel.endpoint.clone()).await?);
        self.tunnel = Some(tunnel);
        let client = self.client.as_mut().context("initial socket client")?;
        let thread: ThreadStartResponse = client
            .request_typed(ClientRequest::ThreadStart {
                request_id: RequestId::Integer(1),
                params: ThreadStartParams {
                    cwd: Some(OWNER_CWD.into()),
                    model: Some("gpt-5.6-terra".into()),
                    permissions: Some("wops-codex-ops-read".into()),
                    ephemeral: Some(false),
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
                        text: "Consume the fixed socket callback fixture".into(),
                        text_elements: Vec::new(),
                    }],
                    ..Default::default()
                },
            })
            .await?;
        let request = timeout(DEADLINE, async {
            loop {
                match client
                    .next_event()
                    .await
                    .context("initial event stream closed")?
                {
                    AppServerEvent::ServerRequest(request)
                        if matches!(request.as_ref(), ServerRequest::DynamicToolCall { .. }) =>
                    {
                        break Ok::<_, anyhow::Error>(*request);
                    }
                    AppServerEvent::Disconnected { .. } => {
                        anyhow::bail!("socket disconnected before callback")
                    }
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
        assert_eq!(params.namespace.as_deref(), Some("witnessops"));
        assert_eq!(params.tool, "read_head");
        assert_eq!(params.arguments, serde_json::json!({}));
        let identity = DynamicToolCleanupIdentity {
            request_id: request_id.clone(),
            thread_id: params.thread_id.clone(),
            turn_id: params.turn_id.clone(),
            call_id: params.call_id.clone(),
            capability: params
                .cleanup_token
                .clone()
                .context("cleanup capability missing")?,
        };
        let head = self.repo.path().join(".git/HEAD");
        std::fs::remove_file(&head)?;
        anyhow::ensure!(
            Command::new("/usr/bin/mkfifo")
                .arg(&head)
                .env_clear()
                .status()
                .await?
                .success(),
            "fixture FIFO unavailable"
        );
        let mut owner = SovereignCallbackClient::new(
            client.request_handle(),
            self.generation.clone(),
            self.repo.path().to_path_buf(),
            [params.thread_id.clone()],
        )?;
        owner.set_test_completion_gate(Arc::clone(&self.complete));
        self.owner = Some(owner);
        let owner = self.owner.as_mut().context("retained owner")?;
        assert!(owner.dispatch(&self.generation, &request).await?);
        let pid = timeout(DEADLINE, async {
            loop {
                if let Some(pid) = owner.snapshots()[0].process_id {
                    return pid;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        let before = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
        let state = before.rsplit_once(") ").context("process state")?.1;
        assert!(!state.starts_with('Z') && !state.starts_with('X'));
        let interrupt_handle = client.request_handle();
        let interrupt_params = TurnInterruptParams {
            thread_id: identity.thread_id.clone(),
            turn_id: identity.turn_id.clone(),
        };
        let interrupt = tokio::spawn(async move {
            interrupt_handle
                .request_typed::<TurnInterruptResponse>(ClientRequest::TurnInterrupt {
                    request_id: RequestId::Integer(3),
                    params: interrupt_params,
                })
                .await
        });
        let cancellation = timeout(DEADLINE, async {
            loop {
                if let AppServerEvent::ServerNotification(notification) = client.next_event().await.context("cancel event stream closed")? {
                    if let ServerNotification::ItemCompleted(item) = notification.as_ref()
                        && item.thread_id == identity.thread_id
                        && item.turn_id == identity.turn_id
                        && matches!(&item.item.payload, ThreadItemPayload::DynamicToolCall { id, status: DynamicToolCallStatus::Failed, .. } if id == &identity.call_id)
                    { return Ok::<_, anyhow::Error>(*notification); }
                    anyhow::ensure!(!matches!(notification.as_ref(), ServerNotification::TurnCompleted(_)), "turn completed while callback cleanup was withheld");
                }
            }
        }).await??;
        owner.observe_notification(&self.generation, &cancellation)?;
        assert!(!interrupt.is_finished());
        self.tunnel
            .take()
            .context("cuttable connection")?
            .stop()
            .await?;
        timeout(DEADLINE, async {
            loop {
                match client.next_event().await.context("no disconnect event")? {
                    AppServerEvent::Disconnected { .. } => return Ok::<_, anyhow::Error>(()),
                    AppServerEvent::ServerNotification(notification)
                        if matches!(
                            notification.as_ref(),
                            ServerNotification::TurnCompleted(_)
                        ) =>
                    {
                        anyhow::bail!("turn completed before external cleanup")
                    }
                    _ => {}
                }
            }
        })
        .await??;
        assert!(
            timeout(Duration::from_secs(3), interrupt).await??.is_err(),
            "interrupt returned success before the reaped owner acknowledged cleanup"
        );
        owner.disconnected();
        timeout(DEADLINE, async {
            loop {
                if owner.snapshots()[0].reaped {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        assert!(!PathBuf::from(format!("/proc/{pid}")).exists());
        assert_eq!(owner.snapshots()[0].execution_count, 1);
        let replacement = Self::connect(format!("ws://{}", self.address)).await?;
        let handle = replacement.request_handle();
        timeout(DEADLINE, async {
            loop {
                if read_status(&handle, &identity).await? == DynamicToolCleanupStatus::OwnerDetached
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        owner.reconnect(&self.generation, handle.clone())?;
        owner.poll().await;
        assert_ne!(owner.snapshots()[0].state, CallbackState::Acknowledged);
        assert_eq!(
            read_status(&handle, &identity).await?,
            DynamicToolCleanupStatus::OwnerDetached
        );
        self.complete.notify_one();
        timeout(DEADLINE, async {
            loop {
                if owner.poll().await[0].state == CallbackState::Acknowledged {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        assert_eq!(
            read_status(&handle, &identity).await?,
            DynamicToolCleanupStatus::Acknowledged
        );
        assert_eq!(owner.snapshots()[0].execution_count, 1);
        assert!(owner.dispatch(&self.generation, &request).await.is_err());
        let duplicate: DynamicToolCleanupAckResponse = handle
            .request_typed(ClientRequest::DynamicToolCleanupAck {
                request_id: RequestId::Integer(80),
                params: DynamicToolCleanupAckParams {
                    identity,
                    response: DynamicToolCallResponse {
                        content_items: Vec::new(),
                        success: false,
                        cleanup_complete: true,
                    },
                },
            })
            .await?;
        assert!(duplicate.already_acknowledged);
        let previous = self
            .client
            .replace(replacement)
            .context("previous socket client")?;
        // The previous worker already reported Disconnected; joining it does not
        // reconnect or resolve any callback on the old transport.
        let _ = timeout(Duration::from_secs(5), previous.shutdown()).await?;
        Ok(())
    }

    async fn stop(mut self) -> Result<()> {
        let owner_cleanup = if let Some(owner) = self.owner.as_mut() {
            owner.disconnected();
            self.complete.notify_one();
            timeout(Duration::from_secs(5), async {
                loop {
                    let settled = owner.poll().await.into_iter().all(|call| {
                        (call.process_id.is_none() || call.reaped)
                            && matches!(
                                call.state,
                                CallbackState::AckPending
                                    | CallbackState::Acknowledged
                                    | CallbackState::Hold
                            )
                    });
                    if settled {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .context("fixture read_head cleanup did not settle")
        } else {
            Ok(())
        };
        let tunnel_cleanup = if let Some(tunnel) = self.tunnel.take() {
            tunnel.stop().await
        } else {
            Ok(())
        };
        if let Some(client) = self.client.take() {
            let _ = timeout(Duration::from_secs(5), client.shutdown()).await;
        }
        if let Some(pid) = self.process.id() {
            // SAFETY: this still-owned Child PID is the temporary candidate we
            // spawned; SIGTERM requests its normal graceful server shutdown.
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGTERM);
            }
        }
        let server_cleanup = match timeout(Duration::from_secs(10), self.process.wait()).await {
            Ok(result) => result.map(|_| ()).context("candidate wait failed"),
            Err(_) => {
                let _ = self.process.start_kill();
                timeout(Duration::from_secs(5), self.process.wait())
                    .await
                    .context("candidate fixture did not exit after kill")?
                    .map(|_| ())
                    .context("candidate fixture reaping failed")
            }
        };
        let _ = timeout(Duration::from_secs(2), self.stderr_drain).await;
        owner_cleanup?;
        tunnel_cleanup?;
        server_cleanup
    }
}

async fn read_status(
    handle: &AppServerRequestHandle,
    identity: &DynamicToolCleanupIdentity,
) -> Result<DynamicToolCleanupStatus> {
    let response: DynamicToolCleanupReadResponse = timeout(
        Duration::from_secs(3),
        handle.request_typed(ClientRequest::DynamicToolCleanupRead {
            request_id: RequestId::Integer(70),
            params: DynamicToolCleanupReadParams {
                identity: identity.clone(),
            },
        }),
    )
    .await??;
    Ok(response.status)
}

#[tokio::test]
async fn sovereign_callback_socket_disconnect_reconnect_acknowledges_reaped_owner_without_replay()
-> Result<()> {
    let mut fixture = SocketFixture::start().await?;
    let result = AssertUnwindSafe(fixture.exercise()).catch_unwind().await;
    let cleanup = fixture.stop().await;
    cleanup?;
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
