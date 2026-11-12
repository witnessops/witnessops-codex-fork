//! Test-only helpers exposed for cross-crate integration tests.
//!
//! Production code should not depend on this module.
//! We prefer this to using a crate feature to avoid building multiple
//! permutations of the crate.

use std::path::PathBuf;
use std::sync::Arc;

use codex_exec_server::EnvironmentManager;
use codex_extension_api::LoadInstructionsFuture;
use codex_extension_api::LoadedUserInstructions;
use codex_extension_api::UserInstructionsProvider;
use codex_http_client::HttpClientFactory;
use codex_http_client::OutboundProxyPolicy;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_model_provider::create_model_provider;
use codex_model_provider_info::ModelProviderInfo;
use codex_models_manager::bundled_models_response;
use codex_models_manager::collaboration_mode_presets;
use codex_models_manager::manager::SharedModelsManager;
use codex_models_manager::test_support::construct_model_info_offline_for_tests;
use codex_models_manager::test_support::get_model_offline_for_tests;
use codex_protocol::ThreadId;
use codex_protocol::config_types::CollaborationModeMask;
use codex_protocol::mcp::ClientMcpExtensions;
use codex_protocol::mcp::OPENAI_FORM_EXTENSION_ID;
use codex_protocol::openai_models::ModelInfo;
use codex_protocol::openai_models::ModelPreset;
use codex_protocol::protocol::SessionSource;
use once_cell::sync::Lazy;

use crate::ThreadManager;
pub use crate::code_mode_host::ProcessOwnedGrpcCodeModeSessionProvider;
use crate::config::Config;
use crate::responses_metadata::CodexResponsesMetadata;
use crate::responses_metadata::CodexResponsesRequestKind;
use crate::responses_metadata::subagent_header_value;
use crate::responses_metadata::subagent_metadata_kind;
use crate::thread_manager;
use crate::unified_exec;

static TEST_MODEL_PRESETS: Lazy<Vec<ModelPreset>> = Lazy::new(|| {
    let mut response = bundled_models_response()
        .unwrap_or_else(|err| panic!("bundled models.json should parse: {err}"));
    response.models.sort_by_key(|model| model.priority);
    let mut presets: Vec<ModelPreset> = response.models.into_iter().map(Into::into).collect();
    ModelPreset::mark_default_by_picker_visibility(&mut presets);
    presets
});

/// Inspect the same resolved environment configurations used to construct sampling steps.
pub async fn environment_windows_sandbox_types(
    thread: &crate::CodexThread,
) -> Vec<(String, codex_sandboxing::SandboxType)> {
    thread
        .session
        .services
        .turn_environments
        .snapshot()
        .await
        .turn_environments()
        .map(|environment| {
            (
                environment.selection.environment_id.clone(),
                environment.config().windows_sandbox_type,
            )
        })
        .collect()
}

/// Reattaches request-only observations to a completed turn's history for capture assertions.
/// Tests inspect this separately from the destination-filtered HTTP/WS request.
pub async fn history_with_tool_call_metadata(
    thread: &crate::CodexThread,
) -> Vec<codex_protocol::models::ResponseItem> {
    let history = thread.conversation_history_snapshot().await;
    let mut items = history.items().cloned().collect::<Vec<_>>();
    thread
        .session
        .services
        .executed_tool_calls
        .attach_to_prompt(&mut items, &mut Default::default());
    items
}

/// Returns the recorder state used by the next session metadata snapshot.
/// This does not change destination filtering or issue a request.
pub fn mcp_attribution_snapshot(
    thread: &crate::CodexThread,
) -> codex_protocol::mcp::McpAttribution {
    thread
        .session
        .services
        .executed_tool_calls
        .mcp_attribution_snapshot()
}

/// Retains the active turn's timing after completion without exporting telemetry.
/// Returns `None` when there is no running task. The observer reports exclusive
/// durations for the caller's interval in inference, automatic review, local
/// tools, overhead, and unknown order.
pub async fn observe_turn_timing(
    thread: &crate::CodexThread,
) -> Option<
    impl Fn(std::time::Instant, std::time::Instant) -> Option<[std::time::Duration; 5]> + use<>,
> {
    let active = thread.session.active_turn.lock().await;
    let timing = Arc::clone(
        &active
            .as_ref()?
            .task
            .as_ref()?
            .turn_context
            .turn_timing_state,
    );
    Some(move |start, end| {
        timing.snapshot_path(end).map(|path| {
            let totals = path.durations(start, end);
            [
                totals.inference,
                totals.auto_review,
                totals.tools,
                totals.overhead,
                totals.unknown,
            ]
        })
    })
}

/// Test-only provider that supplies no user instructions.
#[derive(Debug, Default)]
pub struct EmptyUserInstructionsProvider;

impl UserInstructionsProvider for EmptyUserInstructionsProvider {
    fn load_user_instructions(&self) -> LoadInstructionsFuture<'_> {
        Box::pin(async { LoadedUserInstructions::default() })
    }
}

pub fn set_thread_manager_test_mode(enabled: bool) {
    thread_manager::set_thread_manager_test_mode_for_tests(enabled);
}

pub fn set_deterministic_process_ids(enabled: bool) {
    unified_exec::set_deterministic_process_ids_for_tests(enabled);
}

pub fn auth_manager_from_auth(auth: CodexAuth) -> Arc<AuthManager> {
    AuthManager::from_auth_for_testing(auth)
}

pub fn auth_manager_from_auth_with_home(auth: CodexAuth, codex_home: PathBuf) -> Arc<AuthManager> {
    AuthManager::from_auth_for_testing_with_home(auth, codex_home)
}

pub fn with_code_mode_host_program(
    thread_manager: ThreadManager,
    host_program: PathBuf,
    config: &crate::config::Config,
) -> ThreadManager {
    thread_manager.with_code_mode_host_program_for_tests(host_program, config)
}

pub fn thread_manager_with_models_provider(
    auth: CodexAuth,
    provider: ModelProviderInfo,
) -> ThreadManager {
    ThreadManager::with_models_provider_for_tests(auth, provider)
}

pub fn thread_manager_with_models_provider_and_home(
    auth: CodexAuth,
    provider: ModelProviderInfo,
    codex_home: PathBuf,
    environment_manager: Arc<EnvironmentManager>,
) -> ThreadManager {
    ThreadManager::with_models_provider_and_home_for_tests(
        auth,
        provider,
        codex_home,
        environment_manager,
    )
}

pub async fn start_thread_with_user_shell_override(
    thread_manager: &ThreadManager,
    config: Config,
    user_shell_override: crate::shell::Shell,
    supports_openai_form_elicitation: bool,
) -> codex_protocol::error::Result<crate::NewThread> {
    thread_manager
        .start_thread_with_user_shell_override_for_tests(
            config,
            user_shell_override,
            ClientMcpExtensions::new(
                supports_openai_form_elicitation
                    .then(|| (OPENAI_FORM_EXTENSION_ID.to_string(), serde_json::json!({}))),
            ),
        )
        .await
}

pub async fn resume_legacy_thread_from_rollout_with_user_shell_override(
    thread_manager: &ThreadManager,
    config: Config,
    rollout_path: PathBuf,
    auth_manager: Arc<AuthManager>,
    user_shell_override: crate::shell::Shell,
    supports_openai_form_elicitation: bool,
) -> codex_protocol::error::Result<crate::NewThread> {
    thread_manager
        .resume_legacy_thread_from_rollout_with_user_shell_override_for_tests(
            config,
            rollout_path,
            auth_manager,
            user_shell_override,
            ClientMcpExtensions::new(
                supports_openai_form_elicitation
                    .then(|| (OPENAI_FORM_EXTENSION_ID.to_string(), serde_json::json!({}))),
            ),
        )
        .await
}

pub fn models_manager_with_provider(
    codex_home: PathBuf,
    auth_manager: Arc<AuthManager>,
    provider: ModelProviderInfo,
) -> SharedModelsManager {
    let provider = create_model_provider(provider, Some(auth_manager));
    provider.models_manager(codex_home, /*config_model_catalog*/ None)
}

pub fn default_http_client_factory() -> HttpClientFactory {
    HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault)
}

pub fn get_model_offline(model: Option<&str>) -> String {
    get_model_offline_for_tests(model)
}

pub fn construct_model_info_offline(model: &str, config: &Config) -> ModelInfo {
    Arc::unwrap_or_clone(
        construct_model_info_offline_for_tests(model, &config.to_models_manager_config()).model,
    )
}

#[derive(Clone, Copy)]
pub enum TestCodexResponsesRequestKind {
    Turn,
    Prewarm,
    WebsocketConnection,
}

#[allow(clippy::too_many_arguments)]
pub fn responses_metadata(
    installation_id: &str,
    session_id: &str,
    thread_id: &str,
    turn_id: Option<&str>,
    window_id: String,
    session_source: &SessionSource,
    parent_thread_id: Option<ThreadId>,
    request_kind: TestCodexResponsesRequestKind,
) -> CodexResponsesMetadata {
    let request_kind = match request_kind {
        TestCodexResponsesRequestKind::Turn => Some(CodexResponsesRequestKind::Turn),
        TestCodexResponsesRequestKind::Prewarm => Some(CodexResponsesRequestKind::Prewarm),
        TestCodexResponsesRequestKind::WebsocketConnection => None,
    };
    CodexResponsesMetadata {
        turn_id: request_kind.and(turn_id.map(ToString::to_string)),
        request_kind,
        parent_thread_id,
        subagent_header: subagent_header_value(session_source),
        subagent_kind: request_kind.and_then(|_| subagent_metadata_kind(session_source)),
        ..CodexResponsesMetadata::new(
            installation_id.to_string(),
            session_id.to_string(),
            thread_id.to_string(),
            window_id,
        )
    }
}

pub fn with_parent_turn(mut metadata: CodexResponsesMetadata, id: &str) -> CodexResponsesMetadata {
    metadata.parent_turn_id = Some(id.to_string());
    metadata
}

pub fn all_model_presets() -> &'static Vec<ModelPreset> {
    &TEST_MODEL_PRESETS
}

pub fn builtin_collaboration_mode_presets() -> Vec<CollaborationModeMask> {
    collaboration_mode_presets::builtin_collaboration_mode_presets()
}

/// Observe the actual speculative tool plan for an idle integration-test thread.
/// Builds a context and tool plan only: no task, model request or tool invocation.
/// Call only with an isolated fixture home and no configured external services.
pub async fn sovereign_tool_inventory_for_test(
    thread: &crate::CodexThread,
) -> codex_protocol::error::Result<serde_json::Value> {
    let turn = thread.session.new_default_turn().await;
    let step = thread
        .session
        .capture_speculative_step_context(turn, &tokio_util::sync::CancellationToken::new())
        .await?;
    let router = &step.tool_router;
    let specs = router.model_visible_specs();
    let nested = router
        .code_mode_tool_names()
        .iter()
        .map(|(identifier, name)| {
            serde_json::json!({
                "identifier": identifier, "namespace": name.namespace, "name": name.name,
            })
        })
        .collect::<Vec<_>>();
    let forbidden = [
        codex_tools::ToolName::plain("exec_command"),
        codex_tools::ToolName::plain("shell"),
        codex_tools::ToolName::plain("shell_command"),
        codex_tools::ToolName::plain("apply_patch"),
        codex_tools::ToolName::plain("web_search"),
        codex_tools::ToolName::plain("tool_search"),
        codex_tools::ToolName::namespaced("mcp_unexpected", "execute"),
        codex_tools::ToolName::namespaced("browser", "navigate"),
        codex_tools::ToolName::namespaced("unexpected_nested", "execute"),
    ];
    let exposures = forbidden
        .iter()
        .map(|name| {
            serde_json::json!({
                "namespace": name.namespace, "name": name.name,
                "exposed": router.exposes_tool(name),
            })
        })
        .collect::<Vec<_>>();
    // Read the startup-captured ceiling used by tool construction, not just
    // the mutable extension attachment inspected by the lifecycle tests.
    let policy = &thread.session.tool_policy;
    let allowed = policy.allowed_tools.as_ref().map(|tools| {
        tools
            .iter()
            .map(|name| serde_json::json!({"namespace": name.namespace, "name": name.name}))
            .collect::<Vec<_>>()
    });
    Ok(serde_json::json!({
        "model": step.settings.model_info.slug,
        "model_declared_tool_mode": step.settings.model_info.tool_mode,
        "effective_tool_mode": router.tool_mode(),
        "requires_code_mode_worker": router.requires_code_mode_worker(),
        "has_terminal_controls": router.has_terminal_controls(),
        "can_manage_children": router.can_manage_children(),
        "public_tool_names": specs.iter().map(|spec| spec.name().to_string()).collect::<Vec<_>>(),
        "model_visible_specs": specs.as_ref(),
        "code_mode_tool_names": nested,
        "forbidden_exposures": exposures,
        "captured_policy": {
            "allowed_tools": allowed,
            "require_managed_sandbox": policy.require_managed_sandbox,
            "require_unified_exec": policy.require_unified_exec,
            "require_code_mode_only": policy.require_code_mode_only,
            "expose_additional_permissions": policy.expose_additional_permissions,
        },
    }))
}

/// Run benign refused calls through the real direct router and nested callback
/// broker of an isolated fork. Does not execute JavaScript or submit model input.
pub async fn sovereign_dispatch_refusals_for_test(
    thread: &crate::CodexThread,
) -> anyhow::Result<serde_json::Value> {
    use crate::function_tool::FunctionCallError;
    use crate::tools::context::ToolPayload;
    use crate::tools::router::ToolCall;
    use crate::tools::router::ToolCallSource;
    use codex_code_mode::CodeModeToolKind;
    use codex_tools::ToolName;
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    let turn = thread.session.new_default_turn().await;
    let step = thread
        .session
        .capture_speculative_step_context(turn, &CancellationToken::new())
        .await?;
    anyhow::ensure!(
        step.settings.model_info.slug == "gpt-5.6-terra",
        "wrong dispatch model"
    );
    anyhow::ensure!(
        step.tool_router.tool_mode() == codex_protocol::openai_models::ToolMode::CodeModeOnly,
        "wrong dispatch tool mode"
    );
    let canary = std::env::temp_dir().join(format!(
        "sovereign-dispatch-canary-{}",
        uuid::Uuid::new_v4()
    ));
    anyhow::ensure!(!canary.exists(), "canary already exists");
    let patch = format!(
        "*** Begin Patch\n*** Add File: {}\n+unexpected-dispatch\n*** End Patch\n",
        canary.display()
    );
    let cases = vec![
        (
            ToolName::plain("exec_command"),
            CodeModeToolKind::Function,
            json!({"cmd":"/usr/bin/true"}),
        ),
        (
            ToolName::plain("shell_command"),
            CodeModeToolKind::Function,
            json!({"command":"/usr/bin/true"}),
        ),
        (
            ToolName::plain("shell"),
            CodeModeToolKind::Function,
            json!({"command":["/usr/bin/true"]}),
        ),
        (
            ToolName::plain("apply_patch"),
            CodeModeToolKind::Freeform,
            json!(patch),
        ),
        (
            ToolName::namespaced("mcp_unexpected", "execute"),
            CodeModeToolKind::Function,
            json!({"probe":"offline"}),
        ),
        (
            ToolName::namespaced("browser", "navigate"),
            CodeModeToolKind::Function,
            json!({"url":"about:blank"}),
        ),
        (
            ToolName::namespaced("unexpected_nested", "execute"),
            CodeModeToolKind::Function,
            json!({}),
        ),
        (
            ToolName::namespaced("witnessops", "write_head"),
            CodeModeToolKind::Function,
            json!({}),
        ),
    ];
    let mut results = Vec::new();
    let mut nested_calls = Vec::new();
    let mut expected_errors = Vec::new();
    for (index, (name, kind, input)) in cases.iter().enumerate() {
        let normalized = name.clone().with_default_namespace();
        let expected = if *kind == CodeModeToolKind::Freeform {
            format!("unsupported custom tool call: {normalized}")
        } else {
            format!("unsupported call: {normalized}")
        };
        let payload = match kind {
            CodeModeToolKind::Function => ToolPayload::Function {
                arguments: input.to_string(),
            },
            CodeModeToolKind::Freeform => ToolPayload::Custom {
                input: input.as_str().unwrap().to_string(),
            },
        };
        let call = ToolCall {
            tool_name: normalized,
            call_id: format!("offline-refusal-{index}-{}", uuid::Uuid::new_v4()),
            payload,
            encrypted_function_args: None,
            native_call: None,
        };
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            step.tool_router.dispatch_tool_call_with_code_mode_result(
                Arc::clone(&thread.session),
                Arc::clone(&step),
                CancellationToken::new(),
                Arc::new(tokio::sync::Mutex::new(
                    crate::turn_diff_tracker::TurnDiffTracker::new(),
                )),
                call,
                ToolCallSource::Direct,
            ),
        )
        .await;
        let (error, exact_refusal) = match result {
            Ok(Err(FunctionCallError::RespondToModel(message))) => {
                let exact = message == expected;
                (message, exact)
            }
            Ok(Err(other)) => (format!("WRONG ERROR CLASS: {other}"), false),
            Ok(Ok(_)) => ("UNEXPECTED SUCCESS".to_string(), false),
            Err(_) => ("DISPATCH TIMEOUT".to_string(), false),
        };
        results.push(
            json!({"route":"direct", "namespace":name.namespace, "name":name.name,
            "expected":expected, "error":error, "exact_refusal":exact_refusal}),
        );
        nested_calls.push(codex_code_mode::CodeModeNestedToolCall {
            cell_id: codex_code_mode::CellId::new(format!(
                "offline-refusal-{}",
                uuid::Uuid::new_v4()
            )),
            runtime_tool_call_id: format!("nested-{index}"),
            tool_name: name.clone(),
            tool_kind: *kind,
            input: Some(input.clone()),
        });
        expected_errors.push(expected);
    }
    let nested = crate::tools::code_mode::sovereign_nested_dispatch_for_test(
        Arc::clone(&thread.session),
        Arc::clone(&step),
        nested_calls,
    )
    .await;
    anyhow::ensure!(
        nested.len() == cases.len(),
        "nested response count mismatch"
    );
    for ((result, expected), (name, _, _)) in nested.into_iter().zip(expected_errors).zip(cases) {
        let (error, exact_refusal) = match result {
            Err(message) => {
                let exact = message == expected;
                (message, exact)
            }
            Ok(_) => ("UNEXPECTED SUCCESS".to_string(), false),
        };
        results.push(
            json!({"route":"nested_callback", "namespace":name.namespace, "name":name.name,
            "expected":expected, "error":error, "exact_refusal":exact_refusal}),
        );
    }
    Ok(
        json!({"model":step.settings.model_info.slug, "tool_mode":step.tool_router.tool_mode(),
        "requests":results, "patch_canary_absent":!canary.exists(),
        "scope":"actual direct router and host nested callback broker; no V8 or model execution"}),
    )
}

/// Exercise the real exec handler, process-owned stdio transport, and V8 runtime.
/// Requires an explicitly injected local helper and an isolated fixture thread.
pub async fn sovereign_v8_refusals_for_test(
    thread: &crate::CodexThread,
) -> anyhow::Result<serde_json::Value> {
    use crate::tools::context::ToolPayload;
    use crate::tools::router::ToolCall;
    use crate::tools::router::ToolCallSource;
    use codex_tools::ToolName;
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    let turn = thread.session.new_default_turn().await;
    let step = thread
        .session
        .capture_speculative_step_context(turn, &CancellationToken::new())
        .await?;
    anyhow::ensure!(
        step.settings.model_info.slug == "gpt-5.6-terra",
        "wrong V8 model"
    );
    anyhow::ensure!(
        step.tool_router.tool_mode() == codex_protocol::openai_models::ToolMode::CodeModeOnly,
        "wrong V8 tool mode"
    );
    let canary = std::env::temp_dir().join(format!("sovereign-v8-canary-{}", uuid::Uuid::new_v4()));
    anyhow::ensure!(!canary.exists(), "V8 canary already exists");
    let patch = format!(
        "*** Begin Patch\n*** Add File: {}\n+unexpected-v8-dispatch\n*** End Patch\n",
        canary.display()
    );
    let cases = json!([
        {"name":"exec_command", "input":{"cmd":"/usr/bin/true"}},
        {"name":"shell_command", "input":{"command":"/usr/bin/true"}},
        {"name":"shell", "input":{"command":["/usr/bin/true"]}},
        {"name":"apply_patch", "input":patch},
        {"name":"mcp_unexpected__execute", "input":{"probe":"offline"}},
        {"name":"browser__navigate", "input":{"url":"about:blank"}},
        {"name":"unexpected_nested__execute", "input":{}},
        {"name":"witnessops__write_head", "input":{}}
    ]);
    let nonce = uuid::Uuid::new_v4().to_string();
    let source = format!(
        "// @exec: {{\"yield_time_ms\": 10000, \"max_output_tokens\": 4000}}\nconst nonce = {};\nconst cases = {};\n{}",
        serde_json::to_string(&nonce)?,
        cases,
        r#"
const observed = [];
for (const item of cases) {
  const candidate = tools[item.name];
  const present = typeof candidate;
  try {
    await candidate(item.input);
    observed.push({name:item.name, present, refused:false, error_name:null, error_message:null});
  } catch (error) {
    observed.push({name:item.name, present, refused:present === "undefined" && error instanceof TypeError,
      error_name:error.name, error_message:error.message});
  }
}
text("SOVEREIGN_V8_RESULT " + JSON.stringify({nonce, arithmetic:6*7,
  available:Object.keys(tools).sort(), catalog:ALL_TOOLS.map(x=>x.name).sort(),
  admitted_read_head_type:typeof tools.witnessops__read_head,
  globals:{process:typeof process, require:typeof require, fetch:typeof fetch},
  observed}));
"#,
    );
    let tracker = Arc::new(tokio::sync::Mutex::new(
        crate::turn_diff_tracker::TurnDiffTracker::new(),
    ));
    let worker = thread
        .session
        .services
        .code_mode_service
        .start_turn_worker(&thread.session, Arc::clone(&step), Arc::clone(&tracker))
        .ok_or_else(|| anyhow::anyhow!("Code Mode dispatch worker was not enabled"))?;
    let execution = step
        .tool_router
        .dispatch_tool_call_with_code_mode_result(
            Arc::clone(&thread.session),
            Arc::clone(&step),
            CancellationToken::new(),
            tracker,
            ToolCall {
                tool_name: ToolName::plain("exec"),
                call_id: format!("offline-v8-{nonce}"),
                payload: ToolPayload::Custom {
                    input: source.clone(),
                },
                encrypted_function_args: None,
                native_call: None,
            },
            ToolCallSource::Direct,
        )
        .await;
    drop(worker);
    let execution =
        execution.map_err(|error| anyhow::anyhow!("V8 exec dispatch failed: {error}"))?;
    let raw = execution.result.log_output();
    anyhow::ensure!(
        execution.result.success_for_logging(),
        "V8 cell failed: {raw}"
    );
    let line = raw
        .lines()
        .find_map(|line| line.strip_prefix("SOVEREIGN_V8_RESULT "))
        .ok_or_else(|| anyhow::anyhow!("V8 execution marker missing: {raw}"))?;
    let observed: serde_json::Value = serde_json::from_str(line)?;
    anyhow::ensure!(
        observed["nonce"] == nonce && observed["arithmetic"] == 42,
        "V8 execution proof mismatch: {observed}"
    );
    anyhow::ensure!(
        observed["available"] == json!(["witnessops__read_head"])
            && observed["catalog"] == json!(["witnessops__read_head"]),
        "V8 tool inventory widened: {observed}"
    );
    anyhow::ensure!(
        observed["admitted_read_head_type"] == "function",
        "admitted binding missing"
    );
    let requests = observed["observed"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("V8 observations missing"))?;
    anyhow::ensure!(requests.len() == 8, "V8 refusal count mismatch");
    for (request, expected) in requests.iter().zip(cases.as_array().unwrap()) {
        anyhow::ensure!(
            request["name"] == expected["name"]
                && request["refused"] == true
                && request["present"] == "undefined"
                && request["error_name"] == "TypeError"
                && request["error_message"]
                    .as_str()
                    .is_some_and(|text| text.contains("not a function")),
            "V8 refusal failed: {request}"
        );
    }
    anyhow::ensure!(!canary.exists(), "refused V8 patch created a file");
    let host = std::path::PathBuf::from(std::env::var("CARGO_TARGET_DIR")?)
        .join("release/codex-code-mode-host");
    let mut processes = Vec::new();
    for entry in std::fs::read_dir("/proc")? {
        let entry = entry?;
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let process = entry.path();
        if std::fs::read_link(process.join("exe")).ok().as_ref() != Some(&host) {
            continue;
        }
        let command = std::fs::read(process.join("cmdline"))?;
        let argv = command
            .split(|byte| *byte == 0)
            .filter(|part| !part.is_empty())
            .map(|part| String::from_utf8_lossy(part).into_owned())
            .collect::<Vec<_>>();
        let stdin = std::fs::read_link(process.join("fd/0"))?
            .to_string_lossy()
            .into_owned();
        let stdout = std::fs::read_link(process.join("fd/1"))?
            .to_string_lossy()
            .into_owned();
        anyhow::ensure!(
            stdin.starts_with("pipe:[") && stdout.starts_with("pipe:["),
            "helper transport is not stdio pipes"
        );
        processes.push(
            json!({"pid_in_test_namespace":pid,"executable":host,"argv":argv,
            "stdin":stdin,"stdout":stdout,
            "network_namespace":std::fs::read_link(process.join("ns/net"))?.to_string_lossy()}),
        );
    }
    anyhow::ensure!(
        processes.len() == 1,
        "expected one real helper process, saw {processes:?}"
    );
    Ok(
        json!({"model":step.settings.model_info.slug,"tool_mode":step.tool_router.tool_mode(),
        "worker_result":observed,"raw_exec_output":raw,"javascript":source,"helper_processes":processes,
        "patch_canary_absent":true,
        "scope":"actual exec handler, process-owned stdio transport and V8; absent tool bindings refuse before nested callback; no model inference"}),
    )
}

struct SovereignCancellationTask {
    abort_observer: Option<Arc<dyn Fn() + Send + Sync>>,
    ready: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
}

impl crate::tasks::SessionTask for SovereignCancellationTask {
    fn kind(&self) -> crate::state::TaskKind {
        crate::state::TaskKind::Regular
    }
    fn span_name(&self) -> &'static str {
        "sovereign_offline_cancellation_fixture"
    }
    async fn abort(
        &self,
        _session: Arc<crate::session::session::Session>,
        _ctx: Arc<crate::session::turn_context::TurnContext>,
    ) {
        // Pure observation at production handle_task_abort's cleanup boundary.
        // This hook never signals, waits for, or reaps any process.
        if let Some(observer) = &self.abort_observer {
            observer();
        }
    }
    async fn run(
        self: Arc<Self>,
        session: Arc<crate::session::session::Session>,
        ctx: Arc<crate::session::turn_context::TurnContext>,
        _input: Vec<crate::session::TurnInput>,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> crate::tasks::SessionTaskResult {
        // Match RegularTask's public lifecycle before declaring the fixture ready.
        session
            .emit_turn_started(&ctx, crate::state::TaskKind::Regular)
            .await;
        if let Some(ready) = self.ready.lock().unwrap().take() {
            let _ = ready.send(());
        }
        cancellation_token.cancelled().await;
        self.cancelled
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(None)
    }
}

pub struct SovereignCancellationProbe {
    _worker: Option<Box<dyn Send>>,
    pub turn_id: String,
    cell_id: codex_code_mode::CellId,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    started: serde_json::Value,
}

/// Register an idle, non-inference fixture task and yield a real V8 cell.
/// The caller must use the app-server turn/interrupt handler to cancel it.
pub async fn sovereign_start_cancellation_for_test(
    thread: &crate::CodexThread,
) -> anyhow::Result<SovereignCancellationProbe> {
    use crate::tools::context::ToolPayload;
    use crate::tools::router::ToolCall;
    use crate::tools::router::ToolCallSource;
    use tokio_util::sync::CancellationToken;
    let turn = thread.session.new_default_turn().await;
    anyhow::ensure!(
        turn.config
            .features
            .enabled(codex_features::Feature::CodeModeInterrupt),
        "normal Code Mode interruption is disabled"
    );
    let turn_id = turn.sub_id.clone();
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (ready, ready_rx) = tokio::sync::oneshot::channel();
    thread
        .session
        .spawn_task(
            Arc::clone(&turn),
            Vec::new(),
            SovereignCancellationTask {
                abort_observer: None,
                ready: std::sync::Mutex::new(Some(ready)),
                cancelled: Arc::clone(&cancelled),
            },
        )
        .await;
    ready_rx.await?;
    let step = thread
        .session
        .capture_speculative_step_context(turn, &CancellationToken::new())
        .await?;
    anyhow::ensure!(
        step.settings.model_info.slug == "gpt-5.6-terra"
            && step.tool_router.tool_mode()
                == codex_protocol::openai_models::ToolMode::CodeModeOnly,
        "cancellation fixture widened model/tool mode"
    );
    let nonce = uuid::Uuid::new_v4().to_string();
    let source = format!(
        "text({}); yield_control(); await new Promise(() => {{}}); text(\"UNEXPECTED_CELL_CONTINUATION\");",
        serde_json::to_string(&format!("SOVEREIGN_CANCEL_READY {nonce}"))?
    );
    let tracker = Arc::new(tokio::sync::Mutex::new(
        crate::turn_diff_tracker::TurnDiffTracker::new(),
    ));
    let worker = thread
        .session
        .services
        .code_mode_service
        .start_turn_worker(&thread.session, Arc::clone(&step), Arc::clone(&tracker))
        .ok_or_else(|| anyhow::anyhow!("Code Mode dispatch worker missing"))?;
    let result = step
        .tool_router
        .dispatch_tool_call_with_code_mode_result(
            Arc::clone(&thread.session),
            Arc::clone(&step),
            CancellationToken::new(),
            tracker,
            ToolCall {
                tool_name: codex_tools::ToolName::plain("exec"),
                call_id: format!("cancel-cell-{nonce}"),
                payload: ToolPayload::Custom {
                    input: source.clone(),
                },
                encrypted_function_args: None,
                native_call: None,
            },
            ToolCallSource::Direct,
        )
        .await
        .map_err(|error| anyhow::anyhow!("yielding exec failed: {error}"))?;
    drop(worker);
    let raw = result.result.log_output();
    anyhow::ensure!(
        result.result.success_for_logging(),
        "yielding cell failed: {raw}"
    );
    let id = raw
        .lines()
        .find_map(|line| line.strip_prefix("Script running with cell ID "))
        .ok_or_else(|| anyhow::anyhow!("cell did not yield: {raw}"))?;
    anyhow::ensure!(
        raw.contains(&format!("SOVEREIGN_CANCEL_READY {nonce}"))
            && !raw.contains("UNEXPECTED_CELL_CONTINUATION"),
        "yield proof missing: {raw}"
    );
    anyhow::ensure!(
        !cancelled.load(std::sync::atomic::Ordering::Acquire),
        "task cancelled before request"
    );
    Ok(SovereignCancellationProbe {
        _worker: None,
        turn_id,
        cell_id: codex_code_mode::CellId::new(id.to_string()),
        cancelled,
        started: serde_json::json!({"nonce":nonce,"raw_exec_output":raw,"javascript":source,
            "code_mode_interrupt_enabled":true,"cell_yielded":true}),
    })
}

/// Execute the admitted external callback from real V8; no inference or policy override.
pub async fn sovereign_start_callback_cancellation_for_test(
    thread: &crate::CodexThread,
    abort_observer: Arc<dyn Fn() + Send + Sync>,
) -> anyhow::Result<SovereignCancellationProbe> {
    use crate::tools::context::ToolPayload;
    use crate::tools::router::ToolCall;
    use crate::tools::router::ToolCallSource;
    use tokio_util::sync::CancellationToken;
    let turn = thread.session.new_default_turn().await;
    anyhow::ensure!(
        turn.config
            .features
            .enabled(codex_features::Feature::CodeModeInterrupt),
        "normal Code Mode interruption is disabled"
    );
    let turn_id = turn.sub_id.clone();
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (ready, ready_rx) = tokio::sync::oneshot::channel();
    thread
        .session
        .spawn_task(
            Arc::clone(&turn),
            Vec::new(),
            SovereignCancellationTask {
                abort_observer: Some(abort_observer),
                ready: std::sync::Mutex::new(Some(ready)),
                cancelled: Arc::clone(&cancelled),
            },
        )
        .await;
    ready_rx.await?;
    let step = thread
        .session
        .capture_speculative_step_context(turn, &CancellationToken::new())
        .await?;
    anyhow::ensure!(
        step.settings.model_info.slug == "gpt-5.6-terra"
            && step.tool_router.tool_mode()
                == codex_protocol::openai_models::ToolMode::CodeModeOnly,
        "cancellation fixture widened model/tool mode"
    );
    let nonce = uuid::Uuid::new_v4().to_string();
    let source = format!(
        "const callback = tools.witnessops__read_head({{}}); text({}); yield_control(); await callback; text(\"UNEXPECTED_CELL_CONTINUATION\");",
        serde_json::to_string(&format!("SOVEREIGN_CANCEL_READY {nonce}"))?
    );
    let tracker = Arc::new(tokio::sync::Mutex::new(
        crate::turn_diff_tracker::TurnDiffTracker::new(),
    ));
    let worker = thread
        .session
        .services
        .code_mode_service
        .start_turn_worker(&thread.session, Arc::clone(&step), Arc::clone(&tracker))
        .ok_or_else(|| anyhow::anyhow!("Code Mode dispatch worker missing"))?;
    let result = step
        .tool_router
        .dispatch_tool_call_with_code_mode_result(
            Arc::clone(&thread.session),
            Arc::clone(&step),
            CancellationToken::new(),
            tracker,
            ToolCall {
                tool_name: codex_tools::ToolName::plain("exec"),
                call_id: format!("cancel-cell-{nonce}"),
                payload: ToolPayload::Custom {
                    input: source.clone(),
                },
                encrypted_function_args: None,
                native_call: None,
            },
            ToolCallSource::Direct,
        )
        .await
        .map_err(|error| anyhow::anyhow!("yielding exec failed: {error}"))?;
    let raw = result.result.log_output();
    anyhow::ensure!(
        result.result.success_for_logging(),
        "yielding cell failed: {raw}"
    );
    let id = raw
        .lines()
        .find_map(|line| line.strip_prefix("Script running with cell ID "))
        .ok_or_else(|| anyhow::anyhow!("cell did not yield: {raw}"))?;
    anyhow::ensure!(
        raw.contains(&format!("SOVEREIGN_CANCEL_READY {nonce}"))
            && !raw.contains("UNEXPECTED_CELL_CONTINUATION"),
        "yield proof missing: {raw}"
    );
    anyhow::ensure!(
        !cancelled.load(std::sync::atomic::Ordering::Acquire),
        "task cancelled before request"
    );
    Ok(SovereignCancellationProbe {
        _worker: Some(Box::new(worker)),
        turn_id,
        cell_id: codex_code_mode::CellId::new(id.to_string()),
        cancelled,
        started: serde_json::json!({"nonce":nonce,"raw_exec_output":raw,"javascript":source,
            "code_mode_interrupt_enabled":true,"cell_yielded":true}),
    })
}

/// Observe cancellation after the public turn/interrupt response, without invoking terminate here.
pub async fn sovereign_finish_cancellation_for_test(
    thread: &crate::CodexThread,
    probe: SovereignCancellationProbe,
) -> anyhow::Result<serde_json::Value> {
    anyhow::ensure!(
        probe.cancelled.load(std::sync::atomic::Ordering::Acquire),
        "turn cancellation token was not observed"
    );
    let outcome = thread
        .session
        .services
        .code_mode_service
        .wait(
            codex_code_mode::WaitRequest {
                cell_id: probe.cell_id.clone(),
                yield_time_ms: 0,
                max_output_tokens: None,
            },
            None,
        )
        .await
        .map_err(anyhow::Error::msg)?;
    anyhow::ensure!(
        matches!(&outcome, codex_code_mode::WaitOutcome::MissingCell(_)),
        "interrupted cell remains live: {outcome:?}"
    );
    anyhow::ensure!(
        thread.session.active_turn.lock().await.is_none(),
        "interrupted turn remains active"
    );
    let after = sovereign_v8_refusals_for_test(thread).await?;
    Ok(
        serde_json::json!({"turn_id":probe.turn_id,"cell_id":probe.cell_id,"started":probe.started,
        "cancellation_token_observed":true,"wait_after_interrupt":outcome,"active_turn_cleared":true,
        "post_cancel_worker_execution":after,
        "scope":"real turn/interrupt path on a registered idle fixture task, yielded V8 cell and shared helper; no inference"}),
    )
}

#[cfg(target_os = "linux")]
pub struct SovereignHelperExitWatch {
    records: Vec<serde_json::Value>,
    descriptors: Vec<(u32, String, tokio::io::unix::AsyncFd<std::os::fd::OwnedFd>)>,
}

/// Replay a cancelled ID while a different, explicitly yielded V8 cell is live.
/// This observes the production service/stdio host; it does not terminate the replacement.
pub async fn sovereign_replay_cancelled_cell_for_test(
    thread: &crate::CodexThread,
    cancelled_cell_id: &str,
    replacement: &SovereignCancellationProbe,
) -> anyhow::Result<serde_json::Value> {
    use codex_code_mode::CellId;
    use codex_code_mode::RuntimeResponse;
    use codex_code_mode::WaitOutcome;
    use codex_code_mode::WaitRequest;
    use serde_json::json;
    let old_id = CellId::new(cancelled_cell_id.to_owned());
    anyhow::ensure!(
        old_id != replacement.cell_id,
        "replacement reused cancelled public ID"
    );
    let service = &thread.session.services.code_mode_service;
    let mut records = Vec::new();
    for round in 0..3 {
        for operation in ["wait_before", "terminate", "wait_after"] {
            let old_outcome = if operation == "terminate" {
                service.terminate(old_id.clone().into()).await
            } else {
                service
                    .wait(
                        WaitRequest {
                            cell_id: old_id.clone(),
                            yield_time_ms: 0,
                            max_output_tokens: None,
                        },
                        None,
                    )
                    .await
            }
            .map_err(anyhow::Error::msg)?;
            anyhow::ensure!(
                matches!(&old_outcome, WaitOutcome::MissingCell(_)),
                "cancelled ID replay returned a live cell: {old_outcome:?}"
            );
            let old_json = serde_json::to_value(&old_outcome)?;
            anyhow::ensure!(
                !old_json
                    .to_string()
                    .contains("UNEXPECTED_CELL_CONTINUATION"),
                "cancelled JavaScript continued after replay"
            );
            let live_outcome = service
                .wait(
                    WaitRequest {
                        cell_id: replacement.cell_id.clone(),
                        yield_time_ms: 0,
                        max_output_tokens: None,
                    },
                    None,
                )
                .await
                .map_err(anyhow::Error::msg)?;
            anyhow::ensure!(
                matches!(&live_outcome,
                WaitOutcome::LiveCell(RuntimeResponse::Yielded { cell_id, .. })
                    if cell_id == &replacement.cell_id),
                "old ID affected replacement cell: {live_outcome:?}"
            );
            anyhow::ensure!(
                !replacement
                    .cancelled
                    .load(std::sync::atomic::Ordering::Acquire),
                "old ID cancelled replacement task"
            );
            records.push(
                json!({"round":round,"operation":operation,"old_outcome":old_json,
                "replacement_outcome":live_outcome,"replacement_task_not_cancelled":true}),
            );
        }
    }
    Ok(
        json!({"cancelled_cell_id":old_id,"replacement_cell_id":replacement.cell_id,
        "replacement_turn_id":replacement.turn_id,"replacement_started":replacement.started,
        "operations":records,"scope":"Production Code Mode wait/terminate service through the real stdio helper; no model inference or standalone app-server transport"}),
    )
}

#[cfg(target_os = "linux")]
fn sovereign_process_identity(pid: u32) -> anyhow::Result<(u32, String)> {
    let text = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let (_, tail) = text
        .rsplit_once(") ")
        .ok_or_else(|| anyhow::anyhow!("bad proc stat"))?;
    let fields = tail.split_whitespace().collect::<Vec<_>>();
    Ok((fields[1].parse()?, fields[19].to_string()))
}

/// Capture pidfds while the isolated helper is known to be alive. No signals are sent.
#[cfg(target_os = "linux")]
pub fn sovereign_watch_helper_exit_for_test() -> anyhow::Result<SovereignHelperExitWatch> {
    use std::os::fd::FromRawFd;
    let host = std::path::PathBuf::from(std::env::var("CARGO_TARGET_DIR")?)
        .join("release/codex-code-mode-host");
    let mut identities = std::collections::BTreeMap::new();
    let mut roots = Vec::new();
    for entry in std::fs::read_dir("/proc")? {
        let entry = entry?;
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        if let Ok(identity) = sovereign_process_identity(pid) {
            identities.insert(pid, identity);
        }
        if std::fs::read_link(entry.path().join("exe")).ok().as_ref() == Some(&host) {
            roots.push(pid);
        }
    }
    anyhow::ensure!(roots.len() == 1, "expected one helper process: {roots:?}");
    let mut selected = roots.clone();
    let mut index = 0;
    while index < selected.len() {
        let parent = selected[index];
        let children = identities
            .iter()
            .filter(|(_, (ppid, _))| *ppid == parent)
            .map(|(pid, _)| *pid)
            .collect::<Vec<_>>();
        for child in children {
            if !selected.contains(&child) {
                selected.push(child);
            }
        }
        index += 1;
    }
    let mut records = Vec::new();
    let mut descriptors = Vec::new();
    for pid in selected {
        let (_, start) = identities
            .get(&pid)
            .ok_or_else(|| anyhow::anyhow!("missing process identity"))?;
        // SAFETY: pidfd_open accepts a PID and flags; a nonnegative return is a newly owned descriptor.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) };
        anyhow::ensure!(
            fd >= 0,
            "pidfd_open failed: {}",
            std::io::Error::last_os_error()
        );
        // SAFETY: ownership of this newly opened descriptor transfers exactly once to OwnedFd.
        let fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(fd as std::os::fd::RawFd) };
        let mut tids = std::fs::read_dir(format!("/proc/{pid}/task"))?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().to_string_lossy().parse::<u32>().ok())
            .collect::<Vec<_>>();
        tids.sort_unstable();
        records.push(serde_json::json!({"pid":pid,"start_time_ticks":start,"helper":roots.contains(&pid),"thread_ids":tids}));
        descriptors.push((pid, start.clone(), tokio::io::unix::AsyncFd::new(fd)?));
    }
    Ok(SovereignHelperExitWatch {
        records,
        descriptors,
    })
}

/// Wait on kernel process-exit readiness, then observe reaping of the same identities.
/// Called after fixture thread shutdown and dropping the process-owned provider.
#[cfg(target_os = "linux")]
pub async fn sovereign_verify_helper_exit_for_test(
    watch: SovereignHelperExitWatch,
) -> anyhow::Result<serde_json::Value> {
    for (pid, start, fd) in &watch.descriptors {
        let readiness = fd.readable().await?;
        drop(readiness);
        loop {
            match sovereign_process_identity(*pid) {
                Ok((_, current)) if current == *start => tokio::task::yield_now().await,
                _ => break,
            }
        }
    }
    Ok(
        serde_json::json!({"processes":watch.records,"pidfd_exit_observed":true,"same_process_identities_reaped":true,
        "scope":"helper process, its threads, and any OS descendants present at the live snapshot; no arbitrary spawned-tool descendant claim"}),
    )
}
