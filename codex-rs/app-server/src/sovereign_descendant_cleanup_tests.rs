//! Synthetic descendants through production managed process execution.
//! This is a trusted harness, not a model-admitted shell or external tool callback.

use anyhow::Context;
use anyhow::Result;
use serde::Serialize;
use serde_json::json;
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::unix::AsyncFd;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Serialize)]
struct Identity {
    pid: u32,
    ppid: u32,
    pgrp: u32,
    start_time_ticks: u64,
    state: String,
    comm: String,
}

fn identity(pid: u32) -> Result<Identity> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let (_, tail) = stat.rsplit_once(") ").context("invalid proc stat")?;
    let fields = tail.split_whitespace().collect::<Vec<_>>();
    Ok(Identity {
        pid,
        ppid: fields[1].parse()?,
        pgrp: fields[2].parse()?,
        start_time_ticks: fields[19].parse()?,
        state: fields[0].into(),
        comm: std::fs::read_to_string(format!("/proc/{pid}/comm"))?
            .trim()
            .into(),
    })
}

pub(super) struct Watch {
    tree: Vec<Identity>,
    descriptors: Vec<(Identity, AsyncFd<OwnedFd>)>,
}

async fn watch_ready_tree(nonce: &str, detached: bool) -> Result<Watch> {
    loop {
        let mut tree = Vec::new();
        for entry in std::fs::read_dir("/proc")? {
            let entry = entry?;
            let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
                continue;
            };
            let Ok(item) = identity(pid) else { continue };
            if !matches!(
                item.comm.as_str(),
                "wops-d-ready" | "wops-d-child" | "wops-d-grand"
            ) {
                continue;
            }
            let args = std::fs::read(format!("/proc/{pid}/cmdline"))?;
            if !args
                .split(|byte| *byte == 0)
                .any(|arg| arg == nonce.as_bytes())
            {
                continue;
            }
            tree.push(item);
        }
        if let Some(root) = tree.iter().find(|item| item.comm == "wops-d-ready") {
            anyhow::ensure!(tree.len() == 3, "ready signal without all three processes");
            let child = tree
                .iter()
                .find(|item| item.comm == "wops-d-child")
                .context("child missing")?;
            let grandchild = tree
                .iter()
                .find(|item| item.comm == "wops-d-grand")
                .context("grandchild missing")?;
            anyhow::ensure!(
                child.ppid == root.pid && grandchild.ppid == child.pid,
                "wrong tree ancestry"
            );
            anyhow::ensure!(
                tree.iter()
                    .all(|item| matches!(item.state.as_str(), "S" | "R"))
                    && child.pgrp == root.pgrp
                    && if detached {
                        grandchild.pgrp == grandchild.pid && grandchild.pgrp != root.pgrp
                    } else {
                        grandchild.pgrp == root.pgrp
                    },
                "tree is not live with the required process groups"
            );
            let mut selected = tree.clone();
            let mut parent = root.ppid;
            while parent != std::process::id() {
                anyhow::ensure!(
                    parent > 1 && selected.len() < 16,
                    "sandbox ancestry did not reach this harness"
                );
                let item = identity(parent)?;
                parent = item.ppid;
                selected.push(item);
            }
            let mut descriptors = Vec::new();
            for item in selected {
                // SAFETY: pidfd_open only opens a handle for the observed PID. No signal is sent.
                let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, item.pid as libc::pid_t, 0) };
                anyhow::ensure!(fd >= 0, "pidfd_open: {}", std::io::Error::last_os_error());
                // SAFETY: this successful syscall returned a fresh descriptor, owned exactly once.
                let owned = unsafe { OwnedFd::from_raw_fd(fd as i32) };
                anyhow::ensure!(
                    identity(item.pid)?.start_time_ticks == item.start_time_ticks,
                    "identity changed while arming observer"
                );
                let mut pollfd = libc::pollfd {
                    fd: fd as i32,
                    events: libc::POLLIN,
                    revents: 0,
                };
                // SAFETY: a valid single pollfd is supplied; zero timeout only checks current liveness.
                let ready = unsafe { libc::poll(&mut pollfd, 1, 0) };
                anyhow::ensure!(ready == 0, "process already exited before test trigger");
                descriptors.push((item, AsyncFd::new(owned)?));
            }
            return Ok(Watch { tree, descriptors });
        }
        tokio::task::yield_now().await;
    }
}

async fn verify_reaped(watch: &Watch) -> Result<Vec<serde_json::Value>> {
    let mut observations = Vec::new();
    for (item, fd) in &watch.descriptors {
        let readiness = fd.readable().await?;
        drop(readiness);
        let outcome = loop {
            match std::fs::read_to_string(format!("/proc/{}/stat", item.pid)) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break "absent",
                Err(error) => return Err(error.into()),
                Ok(stat) => {
                    let (_, tail) = stat.rsplit_once(") ").context("invalid proc stat")?;
                    let current: u64 = tail
                        .split_whitespace()
                        .nth(19)
                        .context("start time missing")?
                        .parse()?;
                    if current != item.start_time_ticks {
                        break "pid_reused_after_original_reaped";
                    }
                    tokio::task::yield_now().await;
                }
            }
        };
        observations.push(json!({"identity":item,"pidfd_exit_observed":true,"same_identity_reaped":true,"proc_outcome":outcome}));
    }
    Ok(observations)
}

pub(super) async fn probe(
    snapshot: &codex_core::ThreadConfigSnapshot,
    thread_id: &str,
    timeout_case: bool,
) -> Result<()> {
    use codex_core::exec::{ExecCapturePolicy, ExecExpiration, ExecParams, process_exec_tool_call};
    use codex_core::sandboxing::SandboxPermissions;
    use codex_protocol::config_types::WindowsSandboxLevel;
    use codex_protocol::error::{CodexErrorDetails, SandboxErr};

    anyhow::ensure!(!snapshot.full_access, "unexpected full access");
    let nonce = uuid::Uuid::now_v7().to_string();
    let detached = std::env::var("WOPS_DESCENDANT_SETSID").as_deref() == Ok("1");
    let script = DESCENDANT_SCRIPT;
    let cancellation = CancellationToken::new();
    let helper =
        Some(PathBuf::from(std::env::var("CARGO_TARGET_DIR")?).join("release/codex-linux-sandbox"));
    let self_exe = None;
    let expiration = if timeout_case {
        ExecExpiration::TimeoutOrCancellation {
            timeout: Duration::from_secs(5),
            cancellation: cancellation.clone(),
        }
    } else {
        ExecExpiration::Cancellation(cancellation.clone())
    };
    let params = ExecParams {
        command: vec![
            "/usr/bin/python3".into(),
            "-I".into(),
            "-S".into(),
            "-c".into(),
            script.into(),
            nonce.clone(),
            if detached { "1".into() } else { "0".into() },
        ],
        cwd: snapshot.cwd().clone(),
        expiration,
        capture_policy: ExecCapturePolicy::ShellTool,
        env: std::collections::HashMap::from([
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("LANG".into(), "C.UTF-8".into()),
            (
                "CODEX_BWRAP_SHA256".into(),
                std::env::var("CODEX_BWRAP_SHA256")?,
            ),
        ]),
        network: None,
        network_environment_id: None,
        sandbox_permissions: SandboxPermissions::UseDefault,
        windows_sandbox_level: WindowsSandboxLevel::Disabled,
        justification: None,
        arg0: None,
    };
    let execution = process_exec_tool_call(
        params,
        &snapshot.permission_profile,
        snapshot.cwd(),
        &snapshot.workspace_roots,
        &helper,
        &self_exe,
        false,
        None,
    );
    tokio::pin!(execution);
    let armed = tokio::select! {
        result = &mut execution => anyhow::bail!("execution ended before ready: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(15), watch_ready_tree(&nonce, detached)) => match result {
            Ok(armed) => armed,
            Err(error) => Err(anyhow::anyhow!("readiness deadline: {error}")),
        },
    };
    let watch = match armed {
        Ok(watch) => watch,
        Err(error) => {
            cancellation.cancel();
            let _ = execution.await;
            return Err(error);
        }
    };
    println!(
        "SOVEREIGN_DESCENDANT_ARMED {}",
        json!({"nonce":nonce,"detached_grandchild":detached,"timeout":timeout_case,"tree":watch.tree,"watched_processes":watch.descriptors.len()})
    );
    if !timeout_case {
        cancellation.cancel();
    }
    let result = execution.await;
    let (output, outcome) = match result {
        Ok(output) if !timeout_case => (output, "cancelled"),
        Err(error) if timeout_case => match error.details() {
            CodexErrorDetails::Sandbox(SandboxErr::Timeout { output }) => {
                ((**output).clone(), "timed_out")
            }
            _ => anyhow::bail!("unexpected timeout error: {error:?}"),
        },
        other => anyhow::bail!("unexpected execution outcome: {other:?}"),
    };
    anyhow::ensure!(
        output.timed_out == timeout_case,
        "wrong timeout classification"
    );
    anyhow::ensure!(
        output.exit_code == if timeout_case { 124 } else { 1 },
        "wrong exit code"
    );
    anyhow::ensure!(
        cancellation.is_cancelled() != timeout_case,
        "wrong cancellation trigger"
    );
    let live = output
        .stdout
        .text
        .lines()
        .filter_map(|line| line.strip_prefix("SOVEREIGN_DESCENDANT_LIVE "))
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    anyhow::ensure!(
        live.len() == 3,
        "missing child readiness output: {}",
        output.stdout.text
    );
    for item in &watch.tree {
        anyhow::ensure!(
            live.iter().any(|row| row["pid"] == item.pid
                && row["role"] == item.comm
                && row["nonce"] == nonce
                && row["sigterm_ignored"] == true),
            "child identity output mismatch"
        );
    }
    // This guard fails a stuck test. It does not synchronize or terminate the processes.
    let reaping = tokio::time::timeout(Duration::from_secs(10), verify_reaped(&watch))
        .await
        .context("process identities survived production cleanup")??;
    let record = json!({"nonce":nonce,"detached_grandchild":detached,"thread_id":thread_id,"outcome":outcome,"tree":watch.tree,"live_output":live,"reaping":reaping,"exit_code":output.exit_code,"timed_out":output.timed_out,"stderr":output.stderr.text,"permission_profile":snapshot.permission_profile,"observer_sent_signals":false,"verified_before_thread_and_container_teardown":true,"scope":"Trusted host-side production process_exec_tool_call with actual thread permission snapshot; no model turn or external read_head callback"});
    let path = PathBuf::from(std::env::var("WOPS_FS_CANARY_ROOT")?)
        .join(format!("descendant-{outcome}-{nonce}.json"));
    std::fs::write(path, serde_json::to_vec_pretty(&record)?)?;
    println!("SOVEREIGN_DESCENDANT_CLEANUP {record}");
    Ok(())
}

#[tokio::test]
async fn sovereign_descendant_cancellation_kills_and_reaps_tree() -> Result<()> {
    super::run_policy_resume_regression_with_descendants(false, false, Some(false)).await
}

#[tokio::test]
async fn sovereign_descendant_timeout_kills_and_reaps_tree() -> Result<()> {
    super::run_policy_resume_regression_with_descendants(false, false, Some(true)).await
}

const DESCENDANT_SCRIPT: &str = r#"
import ctypes, json, os, signal, sys
nonce = sys.argv[1]
detached = sys.argv[2] == "1"
assert os.read(0, 1) == b"", "command must receive /dev/null, not the lifetime pipe"
libc = ctypes.CDLL(None, use_errno=True)
signal.signal(signal.SIGTERM, signal.SIG_IGN)
read_fd, write_fd = os.pipe()
def ready(role):
    assert libc.prctl(15, role.encode(), 0, 0, 0) == 0
    stat = open('/proc/self/stat').read()
    outer_pid = int(stat.split(' ', 1)[0])
    print('SOVEREIGN_DESCENDANT_LIVE '+json.dumps({'role':role,'nonce':nonce,'pid':outer_pid,'namespace_pid':os.getpid(),'sigterm_ignored':True}), flush=True)
if os.fork() == 0:
    os.close(read_fd)
    if os.fork() == 0:
        if detached:
            os.setsid()
        ready('wops-d-grand')
        os.write(write_fd, b'G')
    else:
        ready('wops-d-child')
        os.write(write_fd, b'C')
    os.close(write_fd)
else:
    os.close(write_fd)
    received = b''
    while len(received) < 2:
        chunk = os.read(read_fd, 2-len(received))
        assert chunk
        received += chunk
    assert set(received) == {ord('C'), ord('G')}
    os.close(read_fd)
    ready('wops-d-ready')
while True:
    signal.pause()
"#;

impl Watch {
    /// Immediate pidfd/proc observation; never sends a signal or waits for exit.
    pub(super) fn sample(&self) -> serde_json::Value {
        use std::os::fd::AsRawFd;
        let rows = self.descriptors.iter().map(|(item, fd)| {
            let mut poll = libc::pollfd { fd: fd.get_ref().as_raw_fd(), events: libc::POLLIN, revents: 0 };
            // SAFETY: one valid pollfd; a zero timeout does not alter the process.
            let ready = unsafe { libc::poll(&mut poll, 1, 0) };
            let observed = identity(item.pid);
            let reaped = match &observed {
                Ok(now) => now.start_time_ticks != item.start_time_ticks,
                Err(error) => error.downcast_ref::<std::io::Error>()
                    .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound),
            };
            let read_error = observed.as_ref().err().map(|error| format!("{error:#}"));
            let current = observed.ok();
            json!({"identity":item,"pidfd_exit_observed":ready == 1 && poll.revents & libc::POLLIN != 0,
                "same_identity_reaped":reaped,"current":current,"proc_error":read_error,"poll_result":ready})
        }).collect::<Vec<_>>();
        let all = rows
            .iter()
            .all(|row| row["pidfd_exit_observed"] == true && row["same_identity_reaped"] == true);
        json!({"processes":rows,"all_exited_and_reaped":all,"observer_sent_signals":false})
    }
}

pub(super) struct CallbackWorker {
    pub(super) watch: std::sync::Arc<Watch>,
    pub(super) cancellation: CancellationToken,
    join: tokio::task::JoinHandle<Result<serde_json::Value>>,
}

impl CallbackWorker {
    pub(super) async fn finish(&mut self) -> Result<serde_json::Value> {
        let output = tokio::time::timeout(Duration::from_secs(15), &mut self.join).await???;
        let reaping =
            tokio::time::timeout(Duration::from_secs(10), verify_reaped(&self.watch)).await??;
        Ok(json!({"output":output,"reaping":reaping}))
    }
}

/// Synthetic implementation of the fixed read_head callback, using production
/// managed execution and the actual host-owned thread's permission snapshot.
/// Tool arguments never contain a command, path, or permission override.
pub(super) async fn start_callback_worker(
    snapshot: codex_core::ThreadConfigSnapshot,
    nonce: String,
    detached: bool,
) -> Result<CallbackWorker> {
    use codex_core::exec::{ExecCapturePolicy, ExecExpiration, ExecParams, process_exec_tool_call};
    use codex_core::sandboxing::SandboxPermissions;
    use codex_protocol::config_types::WindowsSandboxLevel;
    anyhow::ensure!(!snapshot.full_access, "unexpected full access");
    let cancellation = CancellationToken::new();
    let token = cancellation.clone();
    let task_nonce = nonce.clone();
    let mut join = tokio::spawn(async move {
        let helper = Some(
            PathBuf::from(std::env::var("CARGO_TARGET_DIR")?).join("release/codex-linux-sandbox"),
        );
        let params = ExecParams {
            command: vec![
                "/usr/bin/python3".into(),
                "-I".into(),
                "-S".into(),
                "-c".into(),
                DESCENDANT_SCRIPT.into(),
                task_nonce,
                if detached { "1".into() } else { "0".into() },
            ],
            cwd: snapshot.cwd().clone(),
            expiration: ExecExpiration::Cancellation(token),
            capture_policy: ExecCapturePolicy::ShellTool,
            env: std::collections::HashMap::from([
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("LANG".into(), "C.UTF-8".into()),
                (
                    "CODEX_BWRAP_SHA256".into(),
                    std::env::var("CODEX_BWRAP_SHA256")?,
                ),
            ]),
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
            &helper,
            &None,
            false,
            None,
        )
        .await?;
        anyhow::ensure!(
            !output.timed_out && output.exit_code == 1,
            "wrong cancellation result: {output:?}"
        );
        Ok(
            json!({"exit_code":output.exit_code,"timed_out":output.timed_out,
            "stdout":output.stdout.text,"stderr":output.stderr.text}),
        )
    });
    let armed = tokio::select! {
        result = &mut join => anyhow::bail!("callback ended before ready: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(15), watch_ready_tree(&nonce, detached)) => match result { Ok(value) => value, Err(error) => Err(anyhow::anyhow!("callback ready deadline: {error}")) },
    };
    match armed {
        Ok(watch) => {
            println!(
                "SOVEREIGN_DESCENDANT_ARMED {}",
                json!({"nonce":nonce,"detached_grandchild":detached,
                "callback":true,"tree":watch.tree,"watched_processes":watch.descriptors.len()})
            );
            Ok(CallbackWorker {
                watch: std::sync::Arc::new(watch),
                cancellation,
                join,
            })
        }
        Err(error) => {
            cancellation.cancel();
            let _ = join.await;
            Err(error)
        }
    }
}
