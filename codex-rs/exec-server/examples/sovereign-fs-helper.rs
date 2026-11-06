//! Isolated regression entry point for the unchanged production filesystem helper.
//! No server, inference provider, or service is activated.
fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert!(args.len() == 2 && args[1] == codex_exec_server::CODEX_FS_HELPER_ARG1);
    codex_exec_server::run_fs_helper_main();
}
