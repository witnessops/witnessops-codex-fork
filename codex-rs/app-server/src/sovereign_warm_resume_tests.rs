//! Offline regressions for warm and cold policy continuity on a host-created thread.
//! This does not execute a model turn or certify tool dispatch enforcement.

use super::ConnectionSessionState;
use super::MessageProcessor;
use super::message_processor_tracing_tests::build_test_processor_with_state_db_code_mode_and_owner_mode;
use crate::outgoing_message::ConnectionId;
use crate::outgoing_message::OutgoingEnvelope;
use crate::outgoing_message::OutgoingMessage;
use crate::transport::AppServerTransport;
use crate::transport::ConnectionOrigin;
use anyhow::Context;
use anyhow::Result;
use codex_app_server_protocol::InitializeResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::ThreadUnsubscribeResponse;
use codex_app_server_protocol::ThreadUnsubscribeStatus;
use codex_core::StartThreadOptions;
use codex_core::config::Config;
use codex_core::config::ConfigBuilder;
use codex_core::config::ConfigOverrides;
use codex_extension_api::ToolPolicy;
use codex_login::AuthManager;
use codex_protocol::ThreadId;
use codex_tools::ToolName;
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::time::timeout;

// The runner mounts an empty fixture here inside its private namespace.
// The real operator checkout at this path is never mounted or read.
const OWNER_CWD: &str = "/home/karol/WitnessOps/WitnessOps/repos/witnessops-codex-ops";

async fn request<T: serde::de::DeserializeOwned>(
    processor: &Arc<MessageProcessor>,
    outgoing: &mut mpsc::Receiver<OutgoingEnvelope>,
    connection_id: ConnectionId,
    session: &Arc<ConnectionSessionState>,
    id: i64,
    method: &str,
    params: serde_json::Value,
) -> Result<T> {
    processor
        .process_request(
            connection_id,
            serde_json::from_value(json!({"id": id, "method": method, "params": params}))?,
            &AppServerTransport::WebSocket {
                bind_address: "127.0.0.1:0".parse()?,
            },
            Arc::clone(session),
        )
        .await;
    timeout(Duration::from_secs(30), async {
        loop {
            let envelope = outgoing.recv().await.context("outgoing channel closed")?;
            let OutgoingEnvelope::ToConnection {
                connection_id: target,
                message,
                ..
            } = envelope
            else {
                continue;
            };
            if target != connection_id {
                continue;
            }
            match message {
                OutgoingMessage::Response(response) if response.id == RequestId::Integer(id) => {
                    return Ok(serde_json::from_value(serde_json::to_value(
                        response.result,
                    )?)?);
                }
                OutgoingMessage::Error(error) if error.id == RequestId::Integer(id) => {
                    anyhow::bail!("{method} returned an error: {:?}", error.error);
                }
                _ => {}
            }
        }
    })
    .await
    .context("request response timeout")?
}

fn assert_exact_owner_policy(policy: &ToolPolicy) -> Result<()> {
    anyhow::ensure!(
        policy.allowed_tools.as_deref()
            == Some(
                [
                    ToolName::plain("exec"),
                    ToolName::plain("wait"),
                    ToolName::namespaced("witnessops", "read_head"),
                ]
                .as_slice()
            ),
        "policy allowlist changed or widened: {policy:?}"
    );
    anyhow::ensure!(
        policy.require_managed_sandbox,
        "managed sandbox ceiling lost"
    );
    anyhow::ensure!(policy.require_unified_exec, "unified exec ceiling lost");
    anyhow::ensure!(policy.require_code_mode_only, "CodeModeOnly ceiling lost");
    anyhow::ensure!(
        !policy.expose_additional_permissions,
        "permission arguments exposed"
    );
    Ok(())
}

#[tokio::test]
async fn sovereign_warm_resume_preserves_host_owned_tool_policy() -> Result<()> {
    run_policy_resume_regression(false, false).await
}

#[tokio::test]
async fn sovereign_cold_resume_reinjects_host_owned_tool_policy() -> Result<()> {
    run_policy_resume_regression(true, false).await
}

async fn run_policy_resume_regression(cold_resume: bool, probe_filesystem: bool) -> Result<()> {
    run_policy_resume_regression_with_descendants(cold_resume, probe_filesystem, None).await
}

async fn run_policy_resume_regression_with_descendants(
    cold_resume: bool,
    probe_filesystem: bool,
    descendant_timeout: Option<bool>,
) -> Result<()> {
    let home = TempDir::new()?;
    // No token, credential store, mock inference endpoint or model turn is needed.
    // The unused provider endpoint is unreachable in the network-isolated runner.
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
    let canaries = if probe_filesystem {
        let canaries = FsCanaries::create()?;
        canaries.add_fixture_denials(&home.path().join("config.toml"))?;
        Some(canaries)
    } else {
        None
    };
    let config = Arc::new(
        ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .harness_overrides(ConfigOverrides {
                cwd: Some(PathBuf::from(OWNER_CWD)),
                codex_self_exe: if probe_filesystem {
                    Some(
                        PathBuf::from(std::env::var("CARGO_TARGET_DIR")?)
                            .join("release/examples/sovereign-fs-helper"),
                    )
                } else {
                    None
                },
                codex_linux_sandbox_exe: if probe_filesystem {
                    Some(
                        PathBuf::from(std::env::var("CARGO_TARGET_DIR")?)
                            .join("release/codex-linux-sandbox"),
                    )
                } else {
                    None
                },
                ..Default::default()
            })
            .build()
            .await?,
    );
    let auth = AuthManager::shared_from_config(config.as_ref(), false).await?;
    // Real paginated resume needs an indexed local store. The shared tracing
    // harness normally omits SQLite because its tests do not resume threads.
    let state_db = codex_state::StateRuntime::init(
        config.sqlite_config().clone(),
        "offline_policy_fixture".to_string(),
    )
    .await?;
    let (processor, mut outgoing) = build_test_processor_with_state_db_code_mode_and_owner_mode(
        config,
        auth,
        Some(Arc::clone(&state_db)),
        None,
        crate::DynamicToolOwnerMode::SovereignReadHead,
    )
    .await;
    let owner_id = ConnectionId(101);
    let observer_id = ConnectionId(102);
    let owner = Arc::new(ConnectionSessionState::new(ConnectionOrigin::WebSocket));
    let observer = Arc::new(ConnectionSessionState::new(ConnectionOrigin::WebSocket));

    let result: Result<()> = async {
        for (id, connection_id, session) in [(1, owner_id, &owner), (2, observer_id, &observer)] {
            let _: InitializeResponse = request(
                &processor,
                &mut outgoing,
                connection_id,
                session,
                id,
                "initialize",
                json!({"clientInfo": {"name": "offline-policy-regression", "version": "1"},
                    "capabilities": {"experimentalApi": true}}),
            )
            .await?;
            processor
                .connection_initialized(connection_id, session.request_attestation())
                .await;
        }
        let started: ThreadStartResponse = request(
            &processor,
            &mut outgoing,
            owner_id,
            &owner,
            3,
            "thread/start",
            json!({"cwd": OWNER_CWD, "model": "gpt-5.6-terra",
                "permissions": "wops-codex-ops-read", "ephemeral": false}),
        )
        .await?;
        let thread_id = ThreadId::from_string(&started.thread.id)?;
        anyhow::ensure!(
            started.thread.history_mode == codex_app_server_protocol::ThreadHistoryMode::Paginated,
            "regression must exercise the supported paginated store"
        );
        let before = processor
            .thread_processor
            .thread_manager
            .get_thread(thread_id)
            .await?;
        let policy_before = before
            .thread_extension_data()
            .get::<ToolPolicy>()
            .context("host policy missing after real thread/start")?;
        assert_exact_owner_policy(&policy_before)?;
        let settings_before = before.config_snapshot().await;
        anyhow::ensure!(
            !settings_before.full_access,
            "fixture unexpectedly has full access"
        );
        #[cfg(target_os = "linux")]
        if let Some(timeout_case) = descendant_timeout {
            descendant_cleanup::probe(&settings_before, &started.thread.id, timeout_case).await?;
            return Ok(());
        }
        if let Some(canaries) = &canaries {
            canaries
                .probe(&settings_before, "thread-start", &started.thread.id)
                .await?;
        }
        before.ensure_rollout_materialized().await;
        before.flush_rollout().await?;

        // Keep the owner subscribed, so overrides must not turn this rejoin into
        // a shutdown/cold-resume replacement. Exercise both ordinary and widening input.
        for (id, params) in [
            (
                4,
                json!({"threadId": started.thread.id, "excludeTurns": true}),
            ),
            (
                5,
                json!({"threadId": started.thread.id, "excludeTurns": true,
                "model": "not-the-host-model", "permissions": ":danger-full-access"}),
            ),
        ] {
            let resumed: ThreadResumeResponse = request(
                &processor,
                &mut outgoing,
                observer_id,
                &observer,
                id,
                "thread/resume",
                params,
            )
            .await?;
            let after = processor
                .thread_processor
                .thread_manager
                .get_thread(thread_id)
                .await?;
            anyhow::ensure!(
                Arc::ptr_eq(&before, &after),
                "resume replaced the live thread"
            );
            anyhow::ensure!(resumed.thread.id == started.thread.id, "thread id changed");
            anyhow::ensure!(
                resumed.thread.session_id == started.thread.session_id,
                "session changed"
            );
            anyhow::ensure!(
                resumed.model == "gpt-5.6-terra",
                "model override was applied"
            );
            let policy_after = after
                .thread_extension_data()
                .get::<ToolPolicy>()
                .context("host policy missing after warm resume")?;
            anyhow::ensure!(
                Arc::ptr_eq(&policy_before, &policy_after),
                "host policy was replaced"
            );
            assert_exact_owner_policy(&policy_after)?;
            let settings_after = after.config_snapshot().await;
            anyhow::ensure!(
                settings_after.permission_profile == settings_before.permission_profile,
                "effective permission profile changed"
            );
            anyhow::ensure!(
                settings_after.active_permission_profile
                    == settings_before.active_permission_profile,
                "named permission profile changed"
            );
            anyhow::ensure!(
                !settings_after.full_access,
                "warm resume widened filesystem access"
            );
            if let Some(canaries) = &canaries {
                canaries
                    .probe(
                        &settings_after,
                        &format!("warm-resume-{id}"),
                        &started.thread.id,
                    )
                    .await?;
            }
        }

        if cold_resume {
            // Use public unsubscribe requests and the normal idle-unload path.
            // Retain the old Arc only to distinguish a fresh runtime after resume.
            for (id, connection_id, session) in [(6, owner_id, &owner), (7, observer_id, &observer)]
            {
                let response: ThreadUnsubscribeResponse = request(
                    &processor,
                    &mut outgoing,
                    connection_id,
                    session,
                    id,
                    "thread/unsubscribe",
                    json!({"threadId": started.thread.id}),
                )
                .await?;
                anyhow::ensure!(
                    response.status == ThreadUnsubscribeStatus::Unsubscribed,
                    "expected to unsubscribe a live owner/observer"
                );
            }
            timeout(Duration::from_secs(15), async {
                loop {
                    let absent = processor
                        .thread_processor
                        .thread_manager
                        .get_thread(thread_id)
                        .await
                        .is_err();
                    let unloading = processor
                        .thread_processor
                        .pending_thread_unloads
                        .lock()
                        .await
                        .contains_key(&thread_id);
                    if absent && !unloading {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .context("policy-bound thread did not finish unloading")?;

            // A cold client cannot replace the host's model or managed profile.
            for override_params in [
                json!({"permissions": ":danger-full-access"}),
                json!({"model": "gpt-5.4"}),
            ] {
                let mut params = json!({"threadId": started.thread.id, "excludeTurns": true});
                params
                    .as_object_mut()
                    .unwrap()
                    .extend(override_params.as_object().unwrap().clone());
                let refusal = request::<ThreadResumeResponse>(
                    &processor,
                    &mut outgoing,
                    observer_id,
                    &observer,
                    8,
                    "thread/resume",
                    params,
                )
                .await
                .err()
                .context("cold resume accepted a changed owner scope")?;
                anyhow::ensure!(
                    refusal.to_string().contains(
                        "fixed owner model, checkout, and named managed profile required"
                    ),
                    "cold resume failed for an unexpected reason: {refusal}"
                );
                anyhow::ensure!(
                    processor
                        .thread_processor
                        .thread_manager
                        .get_thread(thread_id)
                        .await
                        .is_err(),
                    "rejected cold resume published a runtime"
                );
            }

            // Supply only identity/paging preferences: persisted settings and
            // the host must establish the policy rather than client tool input.
            let resumed: ThreadResumeResponse = request(
                &processor,
                &mut outgoing,
                observer_id,
                &observer,
                9,
                "thread/resume",
                json!({"threadId": started.thread.id, "excludeTurns": true}),
            )
            .await?;
            let after = processor
                .thread_processor
                .thread_manager
                .get_thread(thread_id)
                .await?;
            anyhow::ensure!(
                !Arc::ptr_eq(&before, &after),
                "cold resume reused the old runtime"
            );
            anyhow::ensure!(
                resumed.thread.id == started.thread.id,
                "cold resume changed thread identity"
            );
            // Root session identity is persisted across runtime lifetimes.
            anyhow::ensure!(
                resumed.thread.session_id == started.thread.session_id,
                "cold resume changed persisted session identity"
            );
            let policy_after = after
                .thread_extension_data()
                .get::<ToolPolicy>()
                .context("cold resume fell back to the unrestricted default policy")?;
            anyhow::ensure!(
                !Arc::ptr_eq(&policy_before, &policy_after),
                "cold resume did not create a fresh host policy"
            );
            assert_exact_owner_policy(&policy_after)?;
            let settings_after = after.config_snapshot().await;
            anyhow::ensure!(
                settings_after.model == settings_before.model,
                "cold resume changed the pinned model"
            );
            anyhow::ensure!(
                settings_after.model_provider_id == settings_before.model_provider_id,
                "cold resume changed the provider"
            );
            anyhow::ensure!(
                settings_after.cwd() == settings_before.cwd(),
                "cold resume changed the owner workspace"
            );
            anyhow::ensure!(
                settings_after.permission_profile == settings_before.permission_profile,
                "cold resume changed effective permissions"
            );
            anyhow::ensure!(
                settings_after.active_permission_profile
                    == settings_before.active_permission_profile,
                "cold resume changed the named profile"
            );
            anyhow::ensure!(
                settings_after.approval_policy == settings_before.approval_policy,
                "cold resume changed approval policy"
            );
            anyhow::ensure!(
                !settings_after.full_access,
                "cold resume widened filesystem access"
            );
            if let Some(canaries) = &canaries {
                canaries
                    .probe(&settings_after, "cold-resume", &started.thread.id)
                    .await?;
            }
        }
        Ok(())
    }
    .await;

    drop(outgoing);
    processor.clear_runtime_references();
    processor.shutdown_threads().await;
    processor.drain_background_tasks().await;
    state_db.close().await;
    result
}

// The unrestricted parent is a deliberate in-process adversarial fixture, created
// directly through ThreadManager with no model turn or tool execution. Both fork
// requests below still enter through the real public app-server request handler.
#[tokio::test]
async fn sovereign_fork_injects_exact_host_owned_tool_policy() -> Result<()> {
    run_policy_fork_regression(false, false, false, false, false, false, false).await
}

#[tokio::test]
async fn sovereign_fork_restricts_unrestricted_parent_policy() -> Result<()> {
    run_policy_fork_regression(true, false, false, false, false, false, false).await
}

#[tokio::test]
async fn sovereign_fork_code_mode_only_inventory() -> Result<()> {
    run_policy_fork_regression(false, true, false, false, false, false, false).await
}

#[tokio::test]
async fn sovereign_fork_code_mode_only_inventory_from_unrestricted_parent() -> Result<()> {
    run_policy_fork_regression(true, true, false, false, false, false, false).await
}

#[tokio::test]
async fn sovereign_fork_refuses_direct_and_nested_dispatch() -> Result<()> {
    run_policy_fork_regression(false, false, true, false, false, false, false).await
}

#[tokio::test]
async fn sovereign_fork_refuses_dispatch_from_unrestricted_parent() -> Result<()> {
    run_policy_fork_regression(true, false, true, false, false, false, false).await
}

async fn run_policy_fork_regression(
    unrestricted_parent: bool,
    inspect_inventory: bool,
    probe_dispatch: bool,
    probe_v8: bool,
    probe_cancel: bool,
    probe_replay: bool,
    probe_filesystem: bool,
) -> Result<()> {
    let home = TempDir::new()?;
    // No token, credential store, mock inference endpoint or model turn is needed.
    // The unused provider endpoint is unreachable in the network-isolated runner.
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
    let canaries = if probe_filesystem {
        let canaries = FsCanaries::create()?;
        canaries.add_fixture_denials(&home.path().join("config.toml"))?;
        Some(canaries)
    } else {
        None
    };
    let config = Arc::new(
        ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .harness_overrides(ConfigOverrides {
                cwd: Some(PathBuf::from(OWNER_CWD)),
                codex_self_exe: if probe_filesystem {
                    Some(
                        PathBuf::from(std::env::var("CARGO_TARGET_DIR")?)
                            .join("release/examples/sovereign-fs-helper"),
                    )
                } else {
                    None
                },
                codex_linux_sandbox_exe: if probe_filesystem {
                    Some(
                        PathBuf::from(std::env::var("CARGO_TARGET_DIR")?)
                            .join("release/codex-linux-sandbox"),
                    )
                } else {
                    None
                },
                ..Default::default()
            })
            .build()
            .await?,
    );
    let auth = AuthManager::shared_from_config(config.as_ref(), false).await?;
    // Real paginated resume needs an indexed local store. The shared tracing
    // harness normally omits SQLite because its tests do not resume threads.
    let state_db = codex_state::StateRuntime::init(
        config.sqlite_config().clone(),
        "offline_policy_fixture".to_string(),
    )
    .await?;
    let provider: Option<Arc<dyn codex_code_mode::CodeModeSessionProvider>> =
        if probe_v8 || probe_cancel || probe_replay {
            let host = PathBuf::from(std::env::var("CARGO_TARGET_DIR")?)
                .join("release/codex-code-mode-host");
            anyhow::ensure!(host.is_file(), "isolated Code Mode helper missing");
            Some(Arc::new(
                codex_code_mode::ProcessOwnedCodeModeSessionProvider::with_host_program(host),
            ))
        } else {
            None
        };
    let (processor, mut outgoing) = build_test_processor_with_state_db_code_mode_and_owner_mode(
        Arc::clone(&config),
        auth,
        Some(Arc::clone(&state_db)),
        provider,
        crate::DynamicToolOwnerMode::SovereignReadHead,
    )
    .await;

    let mut cleanup_watch = None;
    let connection_id = ConnectionId(201);
    let session = Arc::new(ConnectionSessionState::new(ConnectionOrigin::WebSocket));
    let result: Result<()> = async {
        let _: InitializeResponse = request(
            &processor,
            &mut outgoing,
            connection_id,
            &session,
            1,
            "initialize",
            json!({"clientInfo": {"name": "offline-fork-regression", "version": "1"},
                "capabilities": {"experimentalApi": true}}),
        )
        .await?;
        processor
            .connection_initialized(connection_id, session.request_attestation())
            .await;
        let parent_id = if unrestricted_parent {
            let mut options = codex_core::StartThreadOptions::new((*config).clone());
            options.thread_extension_init.insert(ToolPolicy::default());
            processor
                .thread_processor
                .thread_manager
                .start_thread(options)
                .await?
                .thread_id
        } else {
            let started: ThreadStartResponse = request(
                &processor,
                &mut outgoing,
                connection_id,
                &session,
                2,
                "thread/start",
                json!({"cwd": OWNER_CWD, "model": "gpt-5.6-terra",
                    "permissions": "wops-codex-ops-read", "ephemeral": false}),
            )
            .await?;
            ThreadId::from_string(&started.thread.id)?
        };
        let parent = processor
            .thread_processor
            .thread_manager
            .get_thread(parent_id)
            .await?;
        let parent_policy = parent
            .thread_extension_data()
            .get::<ToolPolicy>()
            .context("parent policy fixture missing")?;
        let forbidden = [
            ToolName::plain("exec_command"),
            ToolName::plain("shell"),
            ToolName::plain("apply_patch"),
            ToolName::namespaced("mcp_unexpected", "execute"),
            ToolName::namespaced("browser", "navigate"),
            ToolName::namespaced("unexpected_nested", "execute"),
        ];
        if unrestricted_parent {
            anyhow::ensure!(
                parent_policy.allowed_tools.is_none(),
                "parent is not unrestricted"
            );
            anyhow::ensure!(
                forbidden.iter().all(|name| parent_policy.allows(name)),
                "unrestricted negative control does not admit the forbidden names"
            );
        } else {
            assert_exact_owner_policy(&parent_policy)?;
        }
        let parent_settings = parent.config_snapshot().await;
        anyhow::ensure!(
            !parent_settings.full_access,
            "fixture filesystem permissions widened"
        );
        if let Some(canaries) = &canaries {
            canaries.probe(&parent_settings, "fork-parent", &parent_id.to_string()).await?;
        }
        parent.ensure_rollout_materialized().await;
        parent.flush_rollout().await?;

        let mut loaded_before = processor
            .thread_processor
            .thread_manager
            .list_thread_ids()
            .await;
        loaded_before.sort_by_key(ToString::to_string);
        for override_params in [
            json!({"permissions": ":danger-full-access"}),
            json!({"model": "gpt-5.4"}),
        ] {
            let mut params = json!({"threadId": parent_id.to_string(), "excludeTurns": true});
            params.as_object_mut().unwrap().extend(override_params.as_object().unwrap().clone());
            let refusal = request::<codex_app_server_protocol::ThreadForkResponse>(
            &processor,
            &mut outgoing,
            connection_id,
            &session,
            3,
            "thread/fork",
            params,
        )
        .await
        .err()
        .context("fork accepted a changed owner scope")?;
        anyhow::ensure!(
            refusal
                .to_string()
                .contains("fixed owner model, checkout, and named managed profile required"),
            "fork failed for an unexpected reason: {refusal}"
        );
        let mut loaded_after = processor
            .thread_processor
            .thread_manager
            .list_thread_ids()
            .await;
        loaded_after.sort_by_key(ToString::to_string);
        anyhow::ensure!(
            loaded_after == loaded_before,
            "rejected fork registered a runtime"
        );
        }

        let mut child_ids = Vec::new();
        for (id, prediction) in [(4, false), (5, true)] {
            let forked: codex_app_server_protocol::ThreadForkResponse = request(
                &processor,
                &mut outgoing,
                connection_id,
                &session,
                id,
                "thread/fork",
                json!({"threadId": parent_id.to_string(), "excludeTurns": true,
                    "experimentalPredictionMode": prediction, "ephemeral": prediction}),
            )
            .await?;
            let child_id = ThreadId::from_string(&forked.thread.id)?;
            anyhow::ensure!(child_id != parent_id, "fork reused the parent identity");
            anyhow::ensure!(
                !child_ids.contains(&child_id),
                "fork reused another child identity"
            );
            child_ids.push(child_id);
            let child = processor
                .thread_processor
                .thread_manager
                .get_thread(child_id)
                .await?;
            anyhow::ensure!(!Arc::ptr_eq(&parent, &child), "fork reused parent runtime");
            let child_policy = child
                .thread_extension_data()
                .get::<ToolPolicy>()
                .context("fork fell back to unrestricted default policy")?;
            anyhow::ensure!(
                !Arc::ptr_eq(&parent_policy, &child_policy),
                "fork inherited the parent policy object instead of fresh host policy"
            );
            assert_exact_owner_policy(&child_policy)?;
            anyhow::ensure!(
                forbidden.iter().all(|name| !child_policy.allows(name)),
                "fork policy permits an unexpected native or nested tool name"
            );
            if inspect_inventory {
                let inventory =
                    codex_core::test_support::sovereign_tool_inventory_for_test(&child).await?;
                println!(
                    "SOVEREIGN_TOOL_INVENTORY {}",
                    json!({
                        "unrestricted_parent": unrestricted_parent, "prediction": prediction,
                        "inventory": inventory,
                    })
                );
                anyhow::ensure!(
                    inventory["model"] == "gpt-5.6-terra",
                    "inventory uses wrong model"
                );
                anyhow::ensure!(
                    inventory["model_declared_tool_mode"] == "code_mode_only",
                    "bundled Terra model is not code_mode_only"
                );
                anyhow::ensure!(
                    inventory["effective_tool_mode"] == "code_mode_only",
                    "tool mode widened or fell back"
                );
                anyhow::ensure!(
                    inventory["requires_code_mode_worker"] == true,
                    "Code Mode dispatcher missing"
                );
                anyhow::ensure!(
                    inventory["has_terminal_controls"] == false,
                    "native terminal controls exposed"
                );
                anyhow::ensure!(
                    inventory["can_manage_children"] == false,
                    "unadmitted agent tools exposed"
                );
                let mut names = inventory["public_tool_names"]
                    .as_array()
                    .context("missing public tool inventory")?
                    .iter()
                    .map(|v| v.as_str().context("invalid tool name"))
                    .collect::<Result<Vec<_>>>()?;
                names.sort_unstable();
                anyhow::ensure!(
                    names == vec!["exec", "wait"],
                    "unexpected public inventory: {names:?}"
                );
                let nested = inventory["code_mode_tool_names"]
                    .as_array()
                    .context("missing nested inventory")?;
                anyhow::ensure!(nested.len() == 1, "unexpected nested inventory: {nested:?}");
                anyhow::ensure!(
                    nested[0]["namespace"] == "witnessops" && nested[0]["name"] == "read_head",
                    "nested tool is not the fixed read_head"
                );
                anyhow::ensure!(
                    inventory["forbidden_exposures"]
                        .as_array()
                        .context("missing exposure checks")?
                        .iter()
                        .all(|entry| entry["exposed"] == false),
                    "unexpected tool exposed"
                );
                anyhow::ensure!(
                    inventory["captured_policy"]
                        == json!({
                            "allowed_tools": [
                                {"namespace": null, "name": "exec"},
                                {"namespace": null, "name": "wait"},
                                {"namespace": "witnessops", "name": "read_head"},
                            ],
                            "require_managed_sandbox": true,
                            "require_unified_exec": true,
                            "require_code_mode_only": true,
                            "expose_additional_permissions": false,
                        }),
                    "captured startup policy differs from host policy"
                );
            }
            if probe_cancel || probe_replay {
                let probe = codex_core::test_support::sovereign_start_cancellation_for_test(&child).await?;
                if cleanup_watch.is_none() {
                    cleanup_watch = Some(codex_core::test_support::sovereign_watch_helper_exit_for_test()?);
                }
                let _: codex_app_server_protocol::TurnInterruptResponse = request(
                    &processor, &mut outgoing, connection_id, &session, id + 100,
                    "turn/interrupt", json!({"threadId":child_id.to_string(),"turnId":probe.turn_id}),
                ).await?;
                let observations = codex_core::test_support::sovereign_finish_cancellation_for_test(&child, probe).await?;
                if probe_replay {
                    let old_id = observations["cell_id"].as_str().context("cancelled cell ID missing")?;
                    let replacement = codex_core::test_support::sovereign_start_cancellation_for_test(&child).await?;
                    let replay = codex_core::test_support::sovereign_replay_cancelled_cell_for_test(
                        &child, old_id, &replacement,
                    ).await?;
                    let _: codex_app_server_protocol::TurnInterruptResponse = request(
                        &processor, &mut outgoing, connection_id, &session, id + 101,
                        "turn/interrupt", json!({"threadId":child_id.to_string(),"turnId":replacement.turn_id}),
                    ).await?;
                    let replacement_cancelled = codex_core::test_support::sovereign_finish_cancellation_for_test(&child, replacement).await?;
                    println!("SOVEREIGN_REPLAY {}", json!({
                        "unrestricted_parent":unrestricted_parent,"prediction":prediction,
                        "cancelled":observations,"replay":replay,"replacement_cancelled":replacement_cancelled,
                    }));
                } else {
                    println!("SOVEREIGN_CANCELLATION {}", json!({
                        "unrestricted_parent":unrestricted_parent,"prediction":prediction,"observations":observations,
                    }));
                }
            }
            if probe_v8 {
                let observations =
                    codex_core::test_support::sovereign_v8_refusals_for_test(&child).await?;
                println!(
                    "SOVEREIGN_V8_REFUSALS {}",
                    json!({
                        "unrestricted_parent": unrestricted_parent,
                        "prediction": prediction,
                        "observations": observations,
                    })
                );
            }
            if probe_dispatch {
                let observations =
                    codex_core::test_support::sovereign_dispatch_refusals_for_test(&child).await?;
                println!(
                    "SOVEREIGN_DISPATCH_REFUSALS {}",
                    json!({
                        "unrestricted_parent":unrestricted_parent, "prediction":prediction,
                        "observations":observations,
                    })
                );
                let requests = observations["requests"]
                    .as_array()
                    .context("missing dispatch observations")?;
                anyhow::ensure!(
                    requests.len() == 16,
                    "missing direct or nested refusal cases"
                );
                anyhow::ensure!(
                    requests.iter().all(|r| r["exact_refusal"] == true),
                    "a tool was not refused by the expected guard: {observations}"
                );
                anyhow::ensure!(
                    observations["patch_canary_absent"] == true,
                    "patch canary was created"
                );
            }
            let child_settings = child.config_snapshot().await;
            anyhow::ensure!(
                child_settings.forked_from_thread_id == Some(parent_id),
                "fork lineage lost"
            );
            anyhow::ensure!(
                child_settings.model == parent_settings.model,
                "fork changed model"
            );
            anyhow::ensure!(
                child_settings.model_provider_id == parent_settings.model_provider_id,
                "fork changed provider"
            );
            anyhow::ensure!(
                child_settings.cwd() == parent_settings.cwd(),
                "fork changed workspace"
            );
            anyhow::ensure!(
                child_settings.permission_profile == parent_settings.permission_profile,
                "fork changed effective permissions"
            );
            anyhow::ensure!(
                child_settings.active_permission_profile
                    == parent_settings.active_permission_profile,
                "fork changed named profile"
            );
            anyhow::ensure!(
                child_settings.approval_policy == parent_settings.approval_policy,
                "fork changed approval policy"
            );
            anyhow::ensure!(
                !child_settings.full_access,
                "fork widened filesystem access"
            );
            if let Some(canaries) = &canaries {
                canaries.probe(&child_settings, &format!("fork-child-prediction-{prediction}"), &child_id.to_string()).await?;
            }
            anyhow::ensure!(
                child_settings.ephemeral == prediction,
                "wrong fork lifecycle branch"
            );
            let parent_after = processor
                .thread_processor
                .thread_manager
                .get_thread(parent_id)
                .await?;
            anyhow::ensure!(
                Arc::ptr_eq(&parent, &parent_after),
                "fork replaced parent runtime"
            );
            let policy_after = parent_after
                .thread_extension_data()
                .get::<ToolPolicy>()
                .context("parent lost policy")?;
            anyhow::ensure!(
                Arc::ptr_eq(&parent_policy, &policy_after),
                "fork mutated parent policy"
            );
        }
        Ok(())
    }
    .await;

    drop(outgoing);
    processor.clear_runtime_references();
    processor.shutdown_threads().await;
    processor.drain_background_tasks().await;
    state_db.close().await;
    drop(processor);
    if let Some(watch) = cleanup_watch {
        let cleanup =
            codex_core::test_support::sovereign_verify_helper_exit_for_test(watch).await?;
        println!(
            "{} {}",
            if probe_replay {
                "SOVEREIGN_REPLAY_CLEANUP"
            } else {
                "SOVEREIGN_CANCEL_CLEANUP"
            },
            json!({
                "unrestricted_parent":unrestricted_parent,"cleanup":cleanup,
            })
        );
    }
    result
}

#[tokio::test]
async fn sovereign_fork_v8_worker_refusals() -> Result<()> {
    run_policy_fork_regression(false, false, false, true, false, false, false).await
}

#[tokio::test]
async fn sovereign_fork_v8_worker_refusals_from_unrestricted_parent() -> Result<()> {
    run_policy_fork_regression(true, false, false, true, false, false, false).await
}

#[tokio::test]
async fn sovereign_fork_cancellation_cleans_yielded_cell_and_helper() -> Result<()> {
    run_policy_fork_regression(false, false, false, false, true, false, false).await
}

#[tokio::test]
async fn sovereign_fork_cancellation_from_unrestricted_parent() -> Result<()> {
    run_policy_fork_regression(true, false, false, false, true, false, false).await
}

#[tokio::test]
async fn sovereign_fork_cancelled_cell_replay_preserves_replacement() -> Result<()> {
    run_policy_fork_regression(false, false, false, false, false, true, false).await
}

#[tokio::test]
async fn sovereign_fork_cancelled_cell_replay_from_unrestricted_parent() -> Result<()> {
    run_policy_fork_regression(true, false, false, false, false, true, false).await
}

// Synthetic filesystem fixtures are independent of the supervisor's read_head
// callback. The trusted harness invokes production sandbox execution with each
// real thread's effective permission snapshot; no model tool is admitted here.
struct FsCanaries {
    root: PathBuf,
    cases: Vec<(&'static str, PathBuf, bool, String)>,
    files: Vec<(PathBuf, String)>,
}

impl FsCanaries {
    fn create() -> Result<Self> {
        let root = PathBuf::from(std::env::var("WOPS_FS_CANARY_ROOT")?)
            .join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir(&root)?;
        for directory in ["allowed", "denied-dir", "glob"] {
            std::fs::create_dir(root.join(directory))?;
        }
        let mut files = Vec::new();
        for relative in [
            "allowed/plain.txt",
            "glob/visible.txt",
            "denied-file.txt",
            "denied-dir/secret.txt",
            "glob/hidden.sealed",
        ] {
            let path = root.join(relative);
            let token = format!("SYNTHETIC-CANARY-{}\n", uuid::Uuid::new_v4());
            std::fs::write(&path, &token)?;
            files.push((path, token));
        }
        std::os::unix::fs::symlink(
            root.join("denied-file.txt"),
            root.join("allowed/file-alias"),
        )?;
        std::os::unix::fs::symlink(root.join("denied-dir"), root.join("allowed/dir-alias"))?;
        let cases = vec![
            ("allowed-file", files[0].0.clone(), true, files[0].1.clone()),
            (
                "glob-neighbor",
                files[1].0.clone(),
                true,
                files[1].1.clone(),
            ),
            ("denied-file", files[2].0.clone(), false, files[2].1.clone()),
            (
                "denied-directory",
                files[3].0.clone(),
                false,
                files[3].1.clone(),
            ),
            ("denied-glob", files[4].0.clone(), false, files[4].1.clone()),
            (
                "symlink-file",
                root.join("allowed/file-alias"),
                false,
                files[2].1.clone(),
            ),
            (
                "symlink-directory",
                root.join("allowed/dir-alias/secret.txt"),
                false,
                files[3].1.clone(),
            ),
            (
                "dotdot",
                root.join("allowed/../denied-file.txt"),
                false,
                files[2].1.clone(),
            ),
        ];
        Ok(Self { root, cases, files })
    }

    fn add_fixture_denials(&self, config: &std::path::Path) -> Result<()> {
        let mut contents = std::fs::read_to_string(config)?;
        contents.push_str("\n[permissions.wops-codex-ops-read.filesystem]\n");
        for relative in ["denied-file.txt", "denied-dir", "glob/*.sealed"] {
            contents.push_str(&format!(
                "{} = \"deny\"\n",
                serde_json::to_string(&self.root.join(relative))?
            ));
        }
        std::fs::write(config, &contents)?;
        std::fs::write(self.root.join("fixture-config.toml"), contents)?;
        Ok(())
    }

    async fn probe(
        &self,
        snapshot: &codex_core::ThreadConfigSnapshot,
        stage: &str,
        thread_id: &str,
    ) -> Result<()> {
        use codex_core::exec::ExecCapturePolicy;
        use codex_core::exec::ExecParams;
        use codex_core::exec::process_exec_tool_call;
        use codex_core::sandboxing::SandboxPermissions;
        use codex_protocol::config_types::WindowsSandboxLevel;

        anyhow::ensure!(!snapshot.full_access, "filesystem probe has full access");
        anyhow::ensure!(
            snapshot
                .permission_profile
                .file_system_sandbox_policy()
                .has_denied_read_restrictions(),
            "synthetic deny rules did not reach the actual thread"
        );
        // Only paths go into the child. Denied canary contents must never be
        // supplied in its argv, script or environment.
        let spec = serde_json::to_string(
            &self
                .cases
                .iter()
                .map(|(name, path, _, _)| json!({"name": name, "path": path}))
                .collect::<Vec<_>>(),
        )?;
        let script = r#"
import json, sys
spec = json.loads(sys.argv[1])
reads = []
for case in spec:
    row = dict(case)
    try:
        with open(case['path'], 'r') as handle:
            row.update(readable=True, content=handle.read())
    except OSError as error:
        row.update(readable=False, errno=error.errno)
    reads.append(row)
result = {'reads': reads}
if sys.argv[2] == 'sandbox':
    try:
        with open(spec[0]['path'], 'a') as handle:
            handle.write('UNEXPECTED-WRITE')
        result['write_allowed'] = True
    except OSError as error:
        result.update(write_allowed=False, write_errno=error.errno)
print(json.dumps(result))
"#;
        let control = std::process::Command::new("/usr/bin/python3")
            .args(["-I", "-S", "-c", script, &spec, "control"])
            .env_clear()
            .output()?;
        anyhow::ensure!(control.status.success(), "unsandboxed control failed");
        let control_value: serde_json::Value = serde_json::from_slice(&control.stdout)?;
        for (index, (_, _, _, token)) in self.cases.iter().enumerate() {
            anyhow::ensure!(
                control_value["reads"][index]["readable"] == true,
                "control cannot read canary {index}"
            );
            anyhow::ensure!(
                control_value["reads"][index]["content"] == *token,
                "control canary contents differ at {index}"
            );
        }
        let helper =
            PathBuf::from(std::env::var("CARGO_TARGET_DIR")?).join("release/codex-linux-sandbox");
        anyhow::ensure!(helper.is_file(), "candidate Linux sandbox helper missing");
        let env = std::collections::HashMap::from([
            ("PATH".to_string(), "/usr/bin:/bin".to_string()),
            ("LANG".to_string(), "C.UTF-8".to_string()),
            (
                "CODEX_BWRAP_SHA256".to_string(),
                std::env::var("CODEX_BWRAP_SHA256")?,
            ),
        ]);
        let params = ExecParams {
            command: vec![
                "/usr/bin/python3".into(),
                "-I".into(),
                "-S".into(),
                "-c".into(),
                script.into(),
                spec,
                "sandbox".into(),
            ],
            cwd: snapshot.cwd().clone(),
            expiration: 10_000u64.into(),
            capture_policy: ExecCapturePolicy::FullBufferWithExpiration,
            env,
            network: None,
            network_environment_id: None,
            sandbox_permissions: SandboxPermissions::UseDefault,
            windows_sandbox_level: WindowsSandboxLevel::Disabled,
            justification: None,
            arg0: None,
        };
        let output = process_exec_tool_call(
            params,
            &snapshot.permission_profile,
            snapshot.cwd(),
            &snapshot.workspace_roots,
            &Some(helper.clone()),
            &None,
            false,
            None,
        )
        .await?;
        let record = json!({
            "stage": stage, "thread_id": thread_id, "helper": helper,
            "permission_profile": snapshot.permission_profile,
            "control": control_value, "sandbox_exit_code": output.exit_code,
            "sandbox_stdout": output.stdout.text, "sandbox_stderr": output.stderr.text,
        });
        std::fs::write(
            self.root.join(format!("{stage}.json")),
            serde_json::to_vec_pretty(&record)?,
        )?;
        anyhow::ensure!(
            output.exit_code == 0,
            "sandbox probe failed at {stage}: {}",
            output.stderr.text
        );
        let observed: serde_json::Value = serde_json::from_str(&output.stdout.text)?;
        anyhow::ensure!(
            observed["reads"].as_array().map(Vec::len) == Some(self.cases.len()),
            "missing read observations"
        );
        for (index, (name, _, readable, token)) in self.cases.iter().enumerate() {
            let row = &observed["reads"][index];
            anyhow::ensure!(
                row["name"] == *name && row["readable"] == *readable,
                "unexpected read result at {stage}/{name}: {row}"
            );
            if *readable {
                anyhow::ensure!(row["content"] == *token, "allowed canary differs");
            } else {
                anyhow::ensure!(row.get("content").is_none(), "denied content returned");
                anyhow::ensure!(
                    !output.stdout.text.contains(token) && !output.stderr.text.contains(token),
                    "denied canary leaked in captured output"
                );
            }
        }
        anyhow::ensure!(
            observed["write_allowed"] == false,
            "read-only boundary allowed write"
        );
        for (path, token) in &self.files {
            anyhow::ensure!(std::fs::read_to_string(path)? == *token, "canary changed");
        }
        println!(
            "SOVEREIGN_FS_CANARY {}",
            json!({"root": self.root, "stage": stage,
            "thread_id": thread_id, "control_readable": 8, "allowed_reads": 2,
            "denied_reads": 6, "write_denied": true, "canaries_unchanged": true})
        );
        Ok(())
    }
}

#[tokio::test]
async fn sovereign_filesystem_confidentiality_warm_resume() -> Result<()> {
    run_policy_resume_regression(false, true).await
}

#[tokio::test]
async fn sovereign_filesystem_confidentiality_cold_resume() -> Result<()> {
    run_policy_resume_regression(true, true).await
}

#[tokio::test]
async fn sovereign_filesystem_confidentiality_fork() -> Result<()> {
    run_policy_fork_regression(false, false, false, false, false, false, true).await
}

#[tokio::test]
async fn sovereign_filesystem_confidentiality_fork_unrestricted_parent() -> Result<()> {
    run_policy_fork_regression(true, false, false, false, false, false, true).await
}

#[cfg(target_os = "linux")]
#[path = "sovereign_descendant_cleanup_tests.rs"]
mod descendant_cleanup;

#[cfg(target_os = "linux")]
#[path = "sovereign_callback_cancellation_tests.rs"]
mod callback_cancellation;

#[tokio::test]
async fn sovereign_mode_normal_start_resume_and_fork_preserve_caller_settings() -> Result<()> {
    let home = TempDir::new()?;
    let workspace = TempDir::new()?;
    std::fs::write(
        home.path().join("config.toml"),
        r#"
model = "gpt-6-astra"
model_provider = "offline_policy_fixture"
approval_policy = "never"
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
    let config = Arc::new(
        ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .harness_overrides(ConfigOverrides {
                cwd: Some(workspace.path().to_path_buf()),
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
    let (processor, mut outgoing) = build_test_processor_with_state_db_code_mode_and_owner_mode(
        config,
        auth,
        Some(Arc::clone(&state_db)),
        None,
        crate::DynamicToolOwnerMode::Normal,
    )
    .await;
    let connection_id = ConnectionId(401);
    let session = Arc::new(ConnectionSessionState::new(ConnectionOrigin::WebSocket));
    let result: Result<()> = async {
        let initialized: InitializeResponse = request(&processor, &mut outgoing, connection_id, &session, 1, "initialize",
            json!({"clientInfo":{"name":"ordinary-client-regression","version":"1"},"capabilities":{"experimentalApi":true}})).await?;
        anyhow::ensure!(!initialized.sovereign_read_head, "ordinary server claimed owner mode");
        processor.connection_initialized(connection_id, session.request_attestation()).await;
        let started: ThreadStartResponse = request(&processor, &mut outgoing, connection_id, &session, 2, "thread/start",
            json!({"cwd":workspace.path(),"model":"gpt-6-astra","permissions":":read-only","ephemeral":false,
                "dynamicTools":[{"name":"ordinary_fixture","description":"Harmless caller-supplied fixture","inputSchema":{"type":"object","properties":{}}}]})).await?;
        anyhow::ensure!(started.model == "gpt-6-astra", "normal model was pinned to Sovereign");
        let thread_id = ThreadId::from_string(&started.thread.id)?;
        let thread = processor.thread_processor.thread_manager.get_thread(thread_id).await?;
        let settings = thread.config_snapshot().await;
        anyhow::ensure!(settings.cwd().as_path() == workspace.path(), "normal workspace was pinned to Sovereign");
        anyhow::ensure!(thread.thread_extension_data().get::<ToolPolicy>().is_none(), "normal thread inherited owner allowlist");
        thread.ensure_rollout_materialized().await;
        thread.flush_rollout().await?;
        let resumed: ThreadResumeResponse = request(&processor, &mut outgoing, connection_id, &session, 3, "thread/resume",
            json!({"threadId":started.thread.id,"excludeTurns":true})).await?;
        anyhow::ensure!(resumed.model == "gpt-6-astra", "normal resume was pinned to Sovereign");
        let forked: codex_app_server_protocol::ThreadForkResponse = request(&processor, &mut outgoing, connection_id, &session, 4, "thread/fork",
            json!({"threadId":started.thread.id,"excludeTurns":true,"ephemeral":true})).await?;
        anyhow::ensure!(forked.model == "gpt-6-astra", "normal fork was pinned to Sovereign");
        let child = processor.thread_processor.thread_manager.get_thread(ThreadId::from_string(&forked.thread.id)?).await?;
        anyhow::ensure!(child.config_snapshot().await.cwd().as_path() == workspace.path(), "normal fork changed workspace");
        anyhow::ensure!(child.thread_extension_data().get::<ToolPolicy>().is_none(), "normal fork inherited owner allowlist");
        Ok(())
    }.await;
    drop(outgoing);
    processor.clear_runtime_references();
    processor.shutdown_threads().await;
    processor.drain_background_tasks().await;
    state_db.close().await;
    result
}

// Recovered from the Documents warm-resume/effective-mode lineage. These cases
// use the Nov 3 explicit owner mode and retain its immutable setting locks.
struct OwnerPolicyAuditHarness {
    _home: TempDir,
    config: Arc<Config>,
    state_db: Arc<codex_state::StateRuntime>,
    processor: Arc<MessageProcessor>,
    outgoing: mpsc::Receiver<OutgoingEnvelope>,
    session: Arc<ConnectionSessionState>,
}

impl OwnerPolicyAuditHarness {
    const CONNECTION: ConnectionId = ConnectionId(501);

    async fn new() -> Result<Self> {
        let home = TempDir::new()?;
        std::fs::write(
            home.path().join("config.toml"),
            r#"
model = "gpt-5.6-terra"
model_provider = "offline_policy_fixture"
default_permissions = "wops-codex-ops-read"
approval_policy = "never"
thread_unload_delay_secs = 3600
[permissions.wops-codex-ops-read]
extends = ":read-only"
[model_providers.offline_policy_fixture]
name = "Offline policy audit: no inference"
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
        let config = Arc::new(
            ConfigBuilder::default()
                .codex_home(home.path().to_path_buf())
                .loader_overrides(codex_config::LoaderOverrides::without_managed_config_for_tests())
                .harness_overrides(ConfigOverrides {
                    cwd: Some(PathBuf::from(OWNER_CWD)),
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
        let (processor, outgoing) = build_test_processor_with_state_db_code_mode_and_owner_mode(
            Arc::clone(&config),
            auth,
            Some(Arc::clone(&state_db)),
            None,
            crate::DynamicToolOwnerMode::SovereignReadHead,
        )
        .await;
        processor.models_refresh_worker.shutdown();
        let mut harness = Self {
            _home: home,
            config,
            state_db,
            processor,
            outgoing,
            session: Arc::new(ConnectionSessionState::new(ConnectionOrigin::WebSocket)),
        };
        let initialized = harness
            .request(
                1,
                "initialize",
                json!({
                    "clientInfo": {"name": "immutable-policy-audit", "version": "1"},
                    "capabilities": {"experimentalApi": true}
                }),
            )
            .await?;
        anyhow::ensure!(initialized.get("result").is_some(), "{initialized}");
        harness
            .processor
            .connection_initialized(Self::CONNECTION, harness.session.request_attestation())
            .await;
        Ok(harness)
    }

    async fn request(
        &mut self,
        id: i64,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let transport = AppServerTransport::WebSocket {
            bind_address: "127.0.0.1:0".parse()?,
        };
        let dispatch = self.processor.process_request(
            Self::CONNECTION,
            serde_json::from_value(json!({"id": id, "method": method, "params": params}))?,
            &transport,
            Arc::clone(&self.session),
        );
        let receive = async {
            loop {
                let envelope = self
                    .outgoing
                    .recv()
                    .await
                    .context("outgoing channel closed")?;
                let OutgoingEnvelope::ToConnection {
                    connection_id,
                    message,
                    ..
                } = envelope
                else {
                    continue;
                };
                if connection_id != Self::CONNECTION {
                    continue;
                }
                match message {
                    OutgoingMessage::Response(response)
                        if response.id == RequestId::Integer(id) =>
                    {
                        return Ok(json!({"result": response.result}));
                    }
                    OutgoingMessage::Error(error) if error.id == RequestId::Integer(id) => {
                        return Ok(json!({"error": error.error}));
                    }
                    _ => {}
                }
            }
        };
        timeout(Duration::from_secs(30), async {
            // Drain responses while dispatching, as a real foreground transport does.
            let ((), reply) = tokio::join!(dispatch, receive);
            reply
        })
        .await
        .context("policy audit response timeout")?
    }

    async fn shutdown(self) {
        drop(self.outgoing);
        self.processor.clear_runtime_references();
        self.processor.shutdown_threads().await;
        self.processor.drain_background_tasks().await;
        self.state_db.close().await;
    }
}

fn expected_captured_owner_policy() -> ToolPolicy {
    // Independent expected contract, deliberately not the production constructor.
    ToolPolicy {
        allowed_tools: Some(vec![
            ToolName::plain("exec"),
            ToolName::plain("wait"),
            ToolName::namespaced("witnessops", "read_head"),
        ]),
        require_managed_sandbox: true,
        require_unified_exec: true,
        require_code_mode_only: true,
        expose_additional_permissions: false,
    }
}

#[test_case::test_case("default"; "default_policy")]
#[test_case::test_case("extra_tool"; "extra_native_tool")]
#[test_case::test_case("namespace"; "wrong_tool_namespace")]
#[test_case::test_case("sandbox"; "unmanaged_allowed")]
#[test_case::test_case("unified"; "one_shot_exec_allowed")]
#[test_case::test_case("mode"; "missing_code_mode_only_requirement")]
#[test_case::test_case("permissions"; "additional_permissions_allowed")]
#[test_case::test_case("spoof_store"; "mutable_store_cannot_spoof_captured_policy")]
#[test_case::test_case("model"; "wrong_startup_model")]
#[test_case::test_case("missing_owner"; "missing_host_admission")]
#[test_case::test_case("valid_captured"; "mutable_store_cannot_replace_captured_policy")]
#[tokio::test]
async fn sovereign_warm_resume_audits_captured_policy(variant: &str) -> Result<()> {
    let mut h = OwnerPolicyAuditHarness::new().await?;
    let result: Result<()> = async {
        let mut config = (*h.config).clone();
        let mut policy = expected_captured_owner_policy();
        match variant {
            "default" | "spoof_store" => policy = ToolPolicy::default(),
            "extra_tool" => policy
                .allowed_tools
                .as_mut()
                .unwrap()
                .push(ToolName::plain("exec_command")),
            "namespace" => policy.allowed_tools.as_mut().unwrap()[2] = ToolName::plain("read_head"),
            "sandbox" => policy.require_managed_sandbox = false,
            "unified" => policy.require_unified_exec = false,
            "mode" => policy.require_code_mode_only = false,
            "permissions" => policy.expose_additional_permissions = true,
            "model" => config.model = Some("gpt-5.4".into()),
            "missing_owner" | "valid_captured" => {}
            _ => unreachable!(),
        }
        // Trusted test setup models another internal entry point. It does not
        // let a public client define its policy or manufacture host admission.
        let mut options = StartThreadOptions::new(config);
        options.history_mode = Some(codex_protocol::protocol::ThreadHistoryMode::Legacy);
        options.thread_extension_init.insert(policy);
        if variant != "missing_owner" {
            options
                .thread_extension_init
                .insert(codex_extension_api::SovereignReadHeadOwner);
        }
        let started = h
            .processor
            .thread_processor
            .thread_manager
            .start_thread(options)
            .await?;
        started.thread.ensure_rollout_materialized().await;
        started.thread.flush_rollout().await?;
        if variant == "spoof_store" {
            started
                .thread
                .thread_extension_data()
                .insert(expected_captured_owner_policy());
            anyhow::ensure!(
                started
                    .thread
                    .captured_tool_policy()
                    .allowed_tools
                    .is_none()
            );
        } else if variant == "valid_captured" {
            started
                .thread
                .thread_extension_data()
                .insert(ToolPolicy::default());
            assert_exact_owner_policy(started.thread.captured_tool_policy())?;
        }
        let reply = h
            .request(
                2,
                "thread/resume",
                json!({"threadId": started.thread_id.to_string(), "excludeTurns": true}),
            )
            .await?;
        let after = h
            .processor
            .thread_processor
            .thread_manager
            .get_thread(started.thread_id)
            .await?;
        anyhow::ensure!(
            Arc::ptr_eq(&started.thread, &after),
            "audit replaced the runtime"
        );
        if variant == "valid_captured" {
            anyhow::ensure!(
                reply.pointer("/result/thread/id") == Some(&json!(started.thread_id.to_string())),
                "immutable owner policy rejected: {reply}"
            );
            assert_exact_owner_policy(after.captured_tool_policy())?;
        } else {
            let expected_error = if variant == "missing_owner" {
                "loaded thread owner mode differs from this host"
            } else {
                "running thread does not satisfy the fixed owner policy"
            };
            anyhow::ensure!(reply["error"]["code"] == -32600, "{reply}");
            anyhow::ensure!(reply["error"]["message"] == expected_error, "{reply}");
            anyhow::ensure!(reply.get("result").is_none(), "rejection became success");
        }
        Ok(())
    }
    .await;
    h.shutdown().await;
    result
}

#[test_case::test_case(true; "model")]
#[test_case::test_case(false; "permissions")]
#[tokio::test]
async fn sovereign_warm_resume_retains_live_setting_mutation_locks(
    change_model: bool,
) -> Result<()> {
    let mut h = OwnerPolicyAuditHarness::new().await?;
    let result: Result<()> = async {
        let started = h
            .request(
                2,
                "thread/start",
                json!({"cwd": OWNER_CWD, "historyMode": "legacy"}),
            )
            .await?;
        let id = started
            .pointer("/result/thread/id")
            .and_then(serde_json::Value::as_str)
            .with_context(|| format!("owner start failed: {started}"))?;
        let thread_id = ThreadId::from_string(id)?;
        let thread = h
            .processor
            .thread_processor
            .thread_manager
            .get_thread(thread_id)
            .await?;
        let before = thread.config_snapshot().await;
        let refusal = thread
            .restore_thread_settings(codex_core::CodexThreadSettingsOverrides {
                model: change_model.then(|| "gpt-5.4".into()),
                permission_profile: (!change_model)
                    .then_some(codex_protocol::models::PermissionProfile::Disabled),
                ..Default::default()
            })
            .await
            .err()
            .context("owner runtime accepted changed live settings")?;
        anyhow::ensure!(
            refusal.to_string().contains("sovereign_owner_settings"),
            "{refusal}"
        );
        let after = thread.config_snapshot().await;
        anyhow::ensure!(after.model == before.model, "model changed after refusal");
        anyhow::ensure!(
            after.permission_profile == before.permission_profile,
            "permissions changed after refusal"
        );
        thread.ensure_rollout_materialized().await;
        thread.flush_rollout().await?;
        let resumed = h
            .request(
                3,
                "thread/resume",
                json!({"threadId": thread_id.to_string(), "excludeTurns": true}),
            )
            .await?;
        anyhow::ensure!(resumed.get("result").is_some(), "{resumed}");
        assert_exact_owner_policy(thread.captured_tool_policy())?;
        Ok(())
    }
    .await;
    h.shutdown().await;
    result
}

#[test_case::test_case(true; "admitted_owner")]
#[test_case::test_case(false; "unowned_runtime")]
#[tokio::test]
async fn sovereign_owner_resume_refuses_daemon_recovery(admitted_owner: bool) -> Result<()> {
    let h = OwnerPolicyAuditHarness::new().await?;
    let result: Result<()> = async {
        let mut options = StartThreadOptions::new((*h.config).clone());
        options
            .thread_extension_init
            .insert(expected_captured_owner_policy());
        if admitted_owner {
            options
                .thread_extension_init
                .insert(codex_extension_api::SovereignReadHeadOwner);
        }
        let started = h
            .processor
            .thread_processor
            .thread_manager
            .start_thread(options)
            .await?;
        let reply = h
            .processor
            .thread_processor
            .thread_resume(
                crate::request_processors::ThreadResumeTarget::DaemonRecovery(None),
                codex_app_server_protocol::ThreadResumeParams {
                    thread_id: started.thread_id.to_string(),
                    exclude_turns: true,
                    ..Default::default()
                },
                None,
                None,
                codex_protocol::mcp::ClientMcpExtensions::default(),
            )
            .await;
        let error = reply.err().context("owner host accepted daemon recovery")?;
        anyhow::ensure!(error.code == -32600, "{error:?}");
        anyhow::ensure!(
            error.message == "Sovereign owner mode requires a foreground client",
            "{error:?}"
        );
        let after = h
            .processor
            .thread_processor
            .thread_manager
            .get_thread(started.thread_id)
            .await?;
        anyhow::ensure!(
            Arc::ptr_eq(&started.thread, &after),
            "daemon refusal replaced the runtime"
        );
        Ok(())
    }
    .await;
    h.shutdown().await;
    result
}
