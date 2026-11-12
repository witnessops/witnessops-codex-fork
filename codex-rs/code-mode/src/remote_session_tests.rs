use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use codex_code_mode_protocol::CodeModeSessionProvider;
use pretty_assertions::assert_eq;

use super::ProcessOwnedCodeModeSession;
use super::ProcessOwnedCodeModeSessionProvider;
use super::connection::ConnectionError;
use crate::NoopCodeModeSessionDelegate;

#[test]
fn provider_reuses_its_live_process_host() {
    let provider = ProcessOwnedCodeModeSessionProvider::default();

    let first = provider.process_host();
    let second = provider.process_host();

    assert!(Arc::ptr_eq(&first, &second));
}

#[test]
fn missing_host_error_limits_the_displayed_path_to_512_bytes() {
    let executable = "codex-code-mode-host-does-not-exist";
    let host_program = format!("{}{executable}", "missing-directory/".repeat(/*n*/ 64));
    let expected_suffix = &host_program[host_program.len() - (512 - "...".len())..];
    let error = ConnectionError::Spawn {
        host_program: PathBuf::from(&host_program),
        error: io::Error::new(io::ErrorKind::NotFound, "host unavailable"),
    };

    assert_eq!(
        error.to_string(),
        format!("failed to spawn code-mode host ...{expected_suffix}: host unavailable")
    );
}

#[test]
fn missing_host_error_preserves_utf8_boundaries_when_truncating_the_path() {
    let executable = "codex-code-mode-host-does-not-exist";
    let host_program = format!("{}{executable}", "🦀".repeat(/*n*/ 256));
    let error = ConnectionError::Spawn {
        host_program: PathBuf::from(host_program),
        error: io::Error::new(io::ErrorKind::NotFound, "host unavailable"),
    }
    .to_string();
    let displayed_path = error
        .strip_prefix("failed to spawn code-mode host ")
        .and_then(|message| message.strip_suffix(": host unavailable"))
        .expect("missing-host error should contain the displayed host path");

    assert!(displayed_path.starts_with("..."));
    assert!(displayed_path.ends_with(executable));
    assert!(displayed_path.len() <= 512);
}

#[tokio::test]
async fn provider_returns_missing_host_error() {
    let provider = ProcessOwnedCodeModeSessionProvider::with_host_program(
        "codex-code-mode-host-does-not-exist".into(),
    );

    let error = provider
        .create_session()
        .await
        .err()
        .expect("missing host should fail");

    assert!(error.contains("failed to spawn code-mode host codex-code-mode-host-does-not-exist"));
}

#[tokio::test]
async fn shutdown_before_open_does_not_spawn_the_host() {
    let session = ProcessOwnedCodeModeSession::new();

    session.shutdown().await.expect("shutdown session");
    let error = session
        .execute(
            codex_code_mode_protocol::ExecuteRequest {
                tool_call_id: "call-1".to_string(),
                enabled_tools: Vec::new(),
                source: "text('unreachable')".to_string(),
                yield_time_ms: None,
                max_output_tokens: None,
            },
            Arc::new(NoopCodeModeSessionDelegate),
            /*preempt*/ None,
        )
        .await
        .err()
        .expect("shutdown session should reject execution");

    assert_eq!(error, "code mode session is shutting down");
}

#[cfg(target_os = "linux")]
mod sovereign_transport_timeout {
    use super::super::OwnedCodeModeHost;
    use super::super::ProcessOwnedCodeModeSession;
    use crate::NoopCodeModeSessionDelegate;
    use codex_code_mode_protocol::CodeModeSessionOptions;
    use codex_code_mode_protocol::ExecuteRequest;
    use codex_code_mode_protocol::RuntimeResponse;
    use codex_code_mode_protocol::WaitRequest;
    use serde_json::Value;
    use serde_json::json;
    use std::path::PathBuf;
    use std::process::Stdio;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;
    use tokio::io::AsyncBufReadExt;
    use tokio::io::BufReader;
    use tokio::io::Lines;
    use tokio::process::Child;
    use tokio::process::ChildStdout;
    use tokio::process::Command;

    // The observer only sends SIGSTOP through the pinned pidfd. Production timeout
    // handling must kill/reap the helper; the observer never sends a kill signal.
    const WATCH_SCRIPT: &str = r#"
import os,sys,json,signal,select
pid=int(sys.argv[1]); expected_exe=sys.argv[2]; expected_parent=int(sys.argv[3]); stop=sys.argv[4]=='stop'
def identity():
    raw=open('/proc/%s/stat'%pid).read().rsplit(') ',1)[1].split()
    return raw[0],int(raw[1]),raw[19]
assert os.readlink('/proc/%s/exe'%pid)==expected_exe
state,parent,start=identity(); assert parent==expected_parent
fd=os.pidfd_open(pid,0)
assert identity()[2]==start
if stop:
    signal.pidfd_send_signal(fd,signal.SIGSTOP,None,0)
    while identity()[0]!='T': os.sched_yield()
threads=sorted(int(n) for n in os.listdir('/proc/%s/task'%pid))
print(json.dumps({'phase':'ready','pid':pid,'start_time_ticks':start,'parent_pid':parent,'exe':expected_exe,'pidfd_open':True,'signal':'SIGSTOP' if stop else None,'stopped':identity()[0]=='T','thread_ids':threads}),flush=True)
poll=select.poll(); poll.register(fd,select.POLLIN); events=poll.poll()
assert any(event_fd==fd and flags&select.POLLIN for event_fd,flags in events)
while True:
    try:
        if identity()[2]!=start: break
    except (FileNotFoundError, ProcessLookupError): break
    os.sched_yield()
print(json.dumps({'phase':'exited','pid':pid,'start_time_ticks':start,'pidfd_exit_observed':True,'same_process_identity_reaped':True}),flush=True)
os.close(fd)
"#;

    struct ExitObserver {
        child: Child,
        lines: Lines<BufReader<ChildStdout>>,
        ready: Value,
    }

    fn helper_pid(host: &std::path::Path) -> u32 {
        let pids: Vec<_> = std::fs::read_dir("/proc")
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                std::fs::read_link(entry.path().join("exe")).ok().as_deref() == Some(host)
            })
            .map(|entry| entry.file_name().to_string_lossy().parse::<u32>().unwrap())
            .collect();
        assert_eq!(pids.len(), 1, "expected one isolated helper: {pids:?}");
        pids[0]
    }

    async fn observe(host: &std::path::Path, stop: bool) -> ExitObserver {
        let mut child = Command::new("/usr/bin/python3")
            .args(["-u", "-c", WATCH_SCRIPT])
            .arg(helper_pid(host).to_string())
            .arg(host)
            .arg(std::process::id().to_string())
            .arg(if stop { "stop" } else { "watch" })
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let ready: Value = serde_json::from_str(
            &lines
                .next_line()
                .await
                .unwrap()
                .expect("observer readiness"),
        )
        .unwrap();
        assert_eq!(ready["phase"], "ready");
        assert_eq!(ready["stopped"], stop);
        ExitObserver {
            child,
            lines,
            ready,
        }
    }

    async fn exited(mut observer: ExitObserver) -> Value {
        let result: Value = serde_json::from_str(
            &observer
                .lines
                .next_line()
                .await
                .unwrap()
                .expect("kernel exit observation"),
        )
        .unwrap();
        assert_eq!(result["phase"], "exited");
        assert_eq!(result["pid"], observer.ready["pid"]);
        assert_eq!(
            result["start_time_ticks"],
            observer.ready["start_time_ticks"]
        );
        assert_eq!(result["pidfd_exit_observed"], true);
        assert_eq!(result["same_process_identity_reaped"], true);
        assert!(observer.child.wait().await.unwrap().success());
        json!({"ready":observer.ready,"exit":result})
    }

    async fn exercise(operation: &str) {
        let program = PathBuf::from(std::env::var("CARGO_TARGET_DIR").unwrap())
            .join("release/codex-code-mode-host");
        assert!(program.is_file());
        let host = Arc::new(OwnedCodeModeHost::new(program.clone()));
        let session = ProcessOwnedCodeModeSession::with_host(
            Arc::clone(&host),
            CodeModeSessionOptions::default(),
        );
        let nonce = uuid::Uuid::new_v4().to_string();
        let started=session.execute(ExecuteRequest {
            tool_call_id: format!("timeout-{nonce}"), enabled_tools: Vec::new(),
            source: format!("text({}); yield_control(); await new Promise(() => {{}}); text('UNEXPECTED_TIMEOUT_CONTINUATION');",serde_json::to_string(&nonce).unwrap()),
            yield_time_ms: Some(10000), max_output_tokens: Some(1000),
        },Arc::new(NoopCodeModeSessionDelegate),None).await.unwrap();
        let old_id = started.cell_id.clone();
        let initial = started.initial_response().await.unwrap();
        assert!(matches!(&initial,RuntimeResponse::Yielded { cell_id,.. } if cell_id==&old_id));
        assert!(serde_json::to_string(&initial).unwrap().contains(&nonce));
        let binding = session.connection().await.unwrap();
        assert!(binding.connection.is_alive());
        let observer = observe(&program, true).await;
        println!(
            "SOVEREIGN_TRANSPORT_TIMEOUT_READY {}",
            json!({"operation":operation,"cell_id":old_id,"helper":observer.ready})
        );
        let started_at = Instant::now();
        let result = if operation == "wait" {
            session
                .wait(
                    WaitRequest {
                        cell_id: old_id.clone(),
                        yield_time_ms: 0,
                        max_output_tokens: None,
                    },
                    None,
                )
                .await
        } else {
            session.terminate(old_id.clone().into()).await
        };
        let elapsed = started_at.elapsed();
        let expected = format!("code-mode host timed out waiting for {operation} response");
        assert_eq!(result.unwrap_err(), expected);
        let minimum = if operation == "wait" { 61 } else { 60 };
        assert!(
            elapsed >= Duration::from_secs(minimum),
            "production deadline was shortened: {elapsed:?}"
        );
        assert!(!binding.connection.is_alive());
        binding.cleanup.wait().await;
        assert!(binding.cleanup.is_complete());
        let old_wait = binding
            .connection
            .wait(
                binding.remote.clone(),
                WaitRequest {
                    cell_id: old_id.clone(),
                    yield_time_ms: 0,
                    max_output_tokens: None,
                },
                None,
            )
            .await
            .unwrap_err();
        let old_terminate = binding
            .connection
            .terminate(binding.remote.clone(), old_id.clone().into())
            .await
            .unwrap_err();
        assert_eq!(old_wait, expected);
        assert_eq!(old_terminate, expected);
        // Retain the live logical session, host and old binding until kernel exit proof.
        // Their Drop implementations therefore cannot be the timeout cleanup trigger.
        let timeout_cleanup = exited(observer).await;
        let recovery_nonce = uuid::Uuid::new_v4().to_string();
        let recovery=session.execute(ExecuteRequest {
            tool_call_id: format!("recovery-{recovery_nonce}"),enabled_tools:Vec::new(),
            source: format!("text(JSON.stringify({{nonce:{},answer:6*7,tools:Object.keys(tools).sort()}}));",serde_json::to_string(&recovery_nonce).unwrap()),
            yield_time_ms:Some(10000),max_output_tokens:Some(1000),
        },Arc::new(NoopCodeModeSessionDelegate),None).await.unwrap();
        let recovery_id = recovery.cell_id.clone();
        let recovery_result = recovery.initial_response().await.unwrap();
        assert!(matches!(
            &recovery_result,
            RuntimeResponse::Result {
                error_text: None,
                ..
            }
        ));
        let recovery_text = serde_json::to_string(&recovery_result).unwrap();
        assert!(recovery_text.contains(&recovery_nonce));
        assert!(recovery_text.contains("42"));
        let fresh = session.connection().await.unwrap();
        assert!(fresh.connection.is_alive());
        assert!(!Arc::ptr_eq(&binding.connection, &fresh.connection));
        assert!(fresh.remote.generation > binding.remote.generation);
        let stale_wait = session
            .wait(
                WaitRequest {
                    cell_id: old_id.clone(),
                    yield_time_ms: 0,
                    max_output_tokens: None,
                },
                None,
            )
            .await
            .unwrap_err();
        let stale_terminate = session.terminate(old_id.clone().into()).await.unwrap_err();
        let stale_expected = format!("cell {old_id} belongs to a stale code-mode host generation");
        assert_eq!(stale_wait, stale_expected);
        assert_eq!(stale_terminate, stale_expected);
        let recovery_observer = observe(&program, false).await;
        assert_ne!(
            recovery_observer.ready["pid"],
            timeout_cleanup["ready"]["pid"]
        );
        let old_generation = binding.remote.generation;
        let new_generation = fresh.remote.generation;
        session.shutdown().await.unwrap();
        drop(fresh);
        drop(session);
        drop(host);
        let recovery_cleanup = exited(recovery_observer).await;
        println!(
            "SOVEREIGN_TRANSPORT_TIMEOUT {}",
            json!({
                "operation":operation,"production_deadline_seconds":minimum,"elapsed_seconds":elapsed.as_secs_f64(),
                "timeout_error":expected,"initial_yield":initial,"old_cell_id":old_id,
                "old_binding_alive":binding.connection.is_alive(),"old_session_cleanup_complete":binding.cleanup.is_complete(),
                "old_binding_wait_error":old_wait,"old_binding_terminate_error":old_terminate,
                "timeout_cleanup_before_teardown":timeout_cleanup,
                "old_generation":old_generation,"new_generation":new_generation,"recovery_cell_id":recovery_id,
                "recovery_result":recovery_result,"recovery_nonce":recovery_nonce,
                "stale_wait_error":stale_wait,"stale_terminate_error":stale_terminate,
                "recovery_helper_cleanup":recovery_cleanup,
                "scope":"Real stdio/V8 host suspended with pidfd SIGSTOP; unchanged production transport deadline and supervisor cleanup. No model inference or app-server RPC in this component regression."
            })
        );
    }

    #[tokio::test]
    #[ignore = "requires the isolated offline helper and real 61-second deadline"]
    async fn sovereign_transport_timeout_wait() {
        exercise("wait").await;
    }

    #[tokio::test]
    #[ignore = "requires the isolated offline helper and real 60-second deadline"]
    async fn sovereign_transport_timeout_terminate() {
        exercise("terminate").await;
    }
}
