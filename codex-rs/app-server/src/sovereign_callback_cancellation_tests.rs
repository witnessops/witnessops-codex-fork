//! Offline actual V8 -> DynamicToolHandler -> app-server callback cancellation.
//! The callback body is a synthetic process tree, never a model-supplied command.
use super::descendant_cleanup::CallbackWorker;
use super::descendant_cleanup::Watch;
use super::descendant_cleanup::start_callback_worker;
use super::*;
use std::sync::Mutex;
use std::time::Instant;

async fn next_message(
    outgoing: &mut mpsc::Receiver<OutgoingEnvelope>,
    connection_id: ConnectionId,
) -> Result<serde_json::Value> {
    loop {
        let envelope = outgoing.recv().await.context("outgoing closed")?;
        match envelope {
            OutgoingEnvelope::ToConnection {
                connection_id: target,
                message,
                write_complete_tx,
            } if target == connection_id => {
                if let Some(tx) = write_complete_tx {
                    let _ = tx.send(());
                }
                return Ok(serde_json::to_value(message)?);
            }
            OutgoingEnvelope::Broadcast { message } => return Ok(serde_json::to_value(message)?),
            _ => {}
        }
    }
}

async fn run_callback_regression(detached: bool) -> Result<()> {
    let home = TempDir::new()?;
    std::fs::write(
        home.path().join("config.toml"),
        r#"
model = "gpt-5.6-terra"
model_provider = "offline_policy_fixture"
default_permissions = "wops-codex-ops-read"
approval_policy = "never"
thread_unload_delay_secs = 1

[permissions.wops-codex-ops-read]
extends = ":read-only"

[model_providers.offline_policy_fixture]
name = "Offline policy fixture"
base_url = "http://127.0.0.1:9"
wire_api = "responses"
requires_openai_auth = false
request_max_retries = 0
stream_max_retries = 0

[analytics]
enabled = false

[feedback]
enabled = false
"#,
    )?;
    let target = PathBuf::from(std::env::var("CARGO_TARGET_DIR")?);
    let config = Arc::new(
        ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .harness_overrides(ConfigOverrides {
                cwd: Some(PathBuf::from(OWNER_CWD)),
                codex_self_exe: Some(target.join("release/examples/sovereign-fs-helper")),
                codex_linux_sandbox_exe: Some(target.join("release/codex-linux-sandbox")),
                ..Default::default()
            })
            .build()
            .await?,
    );
    let auth = AuthManager::shared_from_config(config.as_ref(), false).await?;
    let state_db = codex_state::StateRuntime::init(
        config.sqlite_config().clone(),
        "offline_policy_fixture".into(),
    )
    .await?;
    let provider = Arc::new(
        codex_code_mode::ProcessOwnedCodeModeSessionProvider::with_host_program(
            target.join("release/codex-code-mode-host"),
        ),
    );
    let (processor, mut outgoing) = build_test_processor_with_state_db_code_mode_and_owner_mode(
        config,
        auth,
        Some(Arc::clone(&state_db)),
        Some(provider),
        crate::DynamicToolOwnerMode::SovereignReadHead,
    )
    .await;
    let connection_id = ConnectionId(301);
    let session = Arc::new(ConnectionSessionState::new(ConnectionOrigin::WebSocket));
    let mut worker: Option<CallbackWorker> = None;
    let mut acknowledged_cleanup = None;
    let nonce = uuid::Uuid::now_v7().to_string();
    let origin = Instant::now();
    let armed: Arc<Mutex<Option<Arc<Watch>>>> = Arc::new(Mutex::new(None));
    let boundary = Arc::new(Mutex::new(None::<serde_json::Value>));
    let mut evidence = json!({"nonce":nonce,"detached_grandchild":detached,"scope":"actual V8 admitted callback; synthetic external callback implementation; no inference", "events":[]});
    let result: Result<()> = async {
        let _: InitializeResponse = request(&processor, &mut outgoing, connection_id, &session, 1, "initialize",
            json!({"clientInfo":{"name":"offline-callback-regression","version":"1"},"capabilities":{"experimentalApi":true}})).await?;
        processor.connection_initialized(connection_id, session.request_attestation()).await;
        let started: ThreadStartResponse = request(&processor, &mut outgoing, connection_id, &session, 2, "thread/start",
            json!({"cwd":OWNER_CWD,"model":"gpt-5.6-terra","permissions":"wops-codex-ops-read","ephemeral":false})).await?;
        let thread_id = ThreadId::from_string(&started.thread.id)?;
        let thread = processor.thread_processor.thread_manager.get_thread(thread_id).await?;
        let policy = thread.thread_extension_data().get::<ToolPolicy>().context("host policy missing")?;
        assert_exact_owner_policy(&policy)?;
        evidence["thread_id"] = json!(thread_id.to_string());
        evidence["exact_host_policy"] = json!({"allowed_tools":policy.allowed_tools,"require_managed_sandbox":policy.require_managed_sandbox,"require_unified_exec":policy.require_unified_exec,"expose_additional_permissions":policy.expose_additional_permissions});
        let observed_watch = Arc::clone(&armed);
        let observed_boundary = Arc::clone(&boundary);
        let observer = Arc::new(move || {
            let sample = observed_watch.lock().unwrap().as_ref().expect("observer armed before interrupt").sample();
            *observed_boundary.lock().unwrap() = Some(json!({"stage":"SessionTask::abort before TurnAborted lifecycle event",
                "elapsed_ns":origin.elapsed().as_nanos(),"sample":sample}));
        });
        let probe = codex_core::test_support::sovereign_start_callback_cancellation_for_test(&thread, observer).await?;
        evidence["turn_id"] = json!(probe.turn_id);
        let callback = timeout(Duration::from_secs(15), async {
            loop {
                let mut message = next_message(&mut outgoing, connection_id).await?;
                if let Some(params) = message.get_mut("params").and_then(serde_json::Value::as_object_mut) {
                    params.remove("cleanupToken");
                }
                evidence["events"].as_array_mut().unwrap().push(json!({"elapsed_ns":origin.elapsed().as_nanos(),"message":message}));
                if message["method"] == "item/tool/call" { return Ok::<_,anyhow::Error>(message); }
            }
        }).await??;
        anyhow::ensure!(callback["params"]["namespace"] == "witnessops" && callback["params"]["tool"] == "read_head"
            && callback["params"]["arguments"] == json!({}) && callback["params"]["threadId"] == thread_id.to_string()
            && callback["params"]["turnId"] == probe.turn_id, "unexpected callback: {callback}");
        let call_id = callback["params"]["callId"].clone();
        let request_id = callback["id"].clone();
        evidence["callback"] = callback;
        let callback_worker = start_callback_worker(thread.config_snapshot().await, nonce.clone(), detached).await?;
        *armed.lock().unwrap() = Some(Arc::clone(&callback_worker.watch));
        evidence["armed"] = callback_worker.watch.sample();
        anyhow::ensure!(evidence["armed"]["all_exited_and_reaped"] == false, "tree not live");
        worker = Some(callback_worker);
        processor.process_request(connection_id,
            serde_json::from_value(json!({"id":3,"method":"turn/interrupt","params":{"threadId":thread_id.to_string(),"turnId":probe.turn_id}}))?,
            &AppServerTransport::WebSocket{bind_address:"127.0.0.1:0".parse()?}, Arc::clone(&session)).await;
        let mut replied = false;
        let mut completed = false;
        timeout(Duration::from_secs(20), async {
            while !replied || !completed {
                let message = next_message(&mut outgoing, connection_id).await?;
                let callback_worker = worker.as_mut().unwrap();
                // Honor the earliest callback-specific cancellation evidence actually
                // emitted by production. No token is wired directly from the test turn.
                let resolved = message["method"] == "serverRequest/resolved" && message["params"]["requestId"] == request_id;
                let failed = message["method"] == "item/completed" && message["params"]["item"]["id"] == call_id
                    && message["params"]["item"]["status"] == "failed";
                if (resolved || failed) && !callback_worker.cancellation.is_cancelled() {
                    evidence["callback_cancel_trigger"] = json!({"elapsed_ns":origin.elapsed().as_nanos(),"message":message});
                    callback_worker.cancellation.cancel();
                    // The external owner acknowledges only after production managed
                    // cleanup and independent pidfd/proc reaping checks complete.
                    let cleanup = callback_worker.finish().await?;
                    anyhow::ensure!(callback_worker.watch.sample()["all_exited_and_reaped"] == true,
                        "cannot acknowledge a live or unreaped callback");
                    acknowledged_cleanup = Some(cleanup);
                    let call = call_id.as_str().context("call id string")?;
                    anyhow::ensure!(!thread.resolve_dynamic_tool_cleanup("wrong-turn", call, true).await,
                        "wrong turn acknowledged callback");
                    anyhow::ensure!(!thread.resolve_dynamic_tool_cleanup(&probe.turn_id, "wrong-call", true).await,
                        "wrong call acknowledged callback");
                    anyhow::ensure!(!thread.resolve_dynamic_tool_cleanup(&probe.turn_id, call, false).await,
                        "missing acknowledgement released cleanup");
                    // Real elapsed time exceeds the old graceful-abort timeout.
                    // Neither internal teardown nor public completion may occur.
                    processor.process_response(ConnectionId(999), serde_json::from_value(json!({
                        "id":request_id,"result":{"contentItems":[],"success":false,"cleanupComplete":true}
                    }))?).await;
                    let withheld_until = tokio::time::Instant::now() + Duration::from_millis(500);
                    while let Ok(pending) = tokio::time::timeout_at(withheld_until,
                        next_message(&mut outgoing, connection_id)).await {
                        let pending = pending?;
                        anyhow::ensure!(pending["id"] != 3 && pending["method"] != "turn/completed",
                            "teardown released without acknowledgement: {pending}");
                        evidence["events"].as_array_mut().unwrap().push(json!({"elapsed_ns":origin.elapsed().as_nanos(),"message":pending}));
                    }
                    anyhow::ensure!(boundary.lock().unwrap().is_none(), "internal teardown preceded acknowledgement");
                    evidence["ack_withheld"] = json!({"milliseconds":500,"internal_teardown_observed":false,
                        "public_completion_observed":false,"wrong_identity_refused":true,"foreign_connection_refused":true,"missing_ack_refused":true});
                    evidence["cleanup_ack"] = json!({"elapsed_ns":origin.elapsed().as_nanos(),
                        "request_id":request_id,"sample":callback_worker.watch.sample()});
                    processor.process_response(connection_id, serde_json::from_value(json!({
                        "id":request_id,"result":{"contentItems":[],"success":false,"cleanupComplete":true}
                    }))?).await;
                }
                if message["id"] == 3 {
                    anyhow::ensure!(message.get("error").is_none(), "interrupt error: {message}");
                    replied = true;
                    evidence["at_interrupt_response"] = callback_worker.watch.sample();
                }
                if message["method"] == "turn/completed" && message["params"]["turn"]["id"] == probe.turn_id {
                    anyhow::ensure!(message["params"]["turn"]["status"] == "interrupted", "wrong turn terminal status");
                    completed = true;
                    evidence["at_turn_completed"] = callback_worker.watch.sample();
                }
                evidence["events"].as_array_mut().unwrap().push(json!({"elapsed_ns":origin.elapsed().as_nanos(),"message":message}));
            }
            Ok::<_,anyhow::Error>(())
        }).await??;
        evidence["pre_teardown_boundary"] = boundary.lock().unwrap().clone().context("abort boundary not observed")?;
        evidence["cell_after_interrupt"] = codex_core::test_support::sovereign_finish_cancellation_for_test(&thread, probe).await?;
        Ok(())
    }.await;
    // This explicit fixture cleanup occurs AFTER the recorded acceptance boundary.
    // It never turns a failed boundary observation into a passing regression.
    let cleanup = if let Some(cleanup) = acknowledged_cleanup {
        Ok(cleanup)
    } else if let Some(callback_worker) = worker.as_mut() {
        evidence["fixture_cleanup_after_measurement"] = json!({"elapsed_ns":origin.elapsed().as_nanos(),
            "needed_fallback_cancel":!callback_worker.cancellation.is_cancelled(),"before":callback_worker.watch.sample()});
        callback_worker.cancellation.cancel();
        callback_worker.finish().await
    } else {
        Ok(json!({"worker_not_started":true}))
    };
    evidence["cleanup"] = match &cleanup {
        Ok(value) => value.clone(),
        Err(error) => json!({"error":format!("{error:#}")}),
    };
    evidence["execution_error"] = result
        .as_ref()
        .err()
        .map(|error| json!(format!("{error:#}")))
        .unwrap_or(serde_json::Value::Null);
    let passed = result.is_ok()
        && cleanup.is_ok()
        && evidence["ack_withheld"]["internal_teardown_observed"] == false
        && evidence["cleanup_ack"]["sample"]["all_exited_and_reaped"] == true
        && evidence["cleanup_ack"]["elapsed_ns"].as_u64()
            < evidence["pre_teardown_boundary"]["elapsed_ns"].as_u64()
        && evidence["pre_teardown_boundary"]["sample"]["all_exited_and_reaped"] == true
        && evidence["at_interrupt_response"]["all_exited_and_reaped"] == true
        && evidence["at_turn_completed"]["all_exited_and_reaped"] == true;
    evidence["passed"] = json!(passed);
    std::fs::write(
        PathBuf::from(std::env::var("WOPS_FS_CANARY_ROOT")?).join(format!("callback-{nonce}.json")),
        serde_json::to_vec_pretty(&evidence)?,
    )?;
    println!("SOVEREIGN_CALLBACK_CANCELLATION {evidence}");
    drop(outgoing);
    processor.clear_runtime_references();
    processor.shutdown_threads().await;
    processor.drain_background_tasks().await;
    state_db.close().await;
    result?;
    cleanup?;
    anyhow::ensure!(
        passed,
        "callback descendants were not exited and reaped before turn teardown; see callback evidence"
    );
    Ok(())
}

#[tokio::test]
async fn sovereign_callback_cancellation_reaps_before_turn_teardown() -> Result<()> {
    run_callback_regression(false).await
}

#[tokio::test]
async fn sovereign_callback_cancellation_reaps_setsid_grandchild_before_turn_teardown() -> Result<()>
{
    run_callback_regression(true).await
}
