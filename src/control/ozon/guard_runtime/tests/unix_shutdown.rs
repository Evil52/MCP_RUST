use super::*;
use std::process::{Child, Command as ProcessCommand, Stdio};

const CHILD_READY: &str = "MCP_OZON_GUARD_SIGNAL_CHILD_READY";
const CHILD_TEST: &str = "control::ozon::guard_runtime::tests::unix_shutdown::unix_shutdown_observes_interrupt_and_termination_in_isolated_children";

struct SignalChild(Child);

impl Drop for SignalChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn signal_child(path: &Path) {
    let mut shutdown = Box::pin(shutdown_signal());
    // Readiness follows registration of both real Unix handlers. Startup logs
    // alone could race registration and let the OS kill the child directly.
    std::future::poll_fn(|context| {
        assert!(shutdown.as_mut().poll(context).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    fs::write(path, "ready").unwrap();
    shutdown.await;
}

async fn assert_signal(signal: &str) {
    let directory = TestDirectory::new();
    let ready = directory.0.join("ready");
    let output = directory.0.join("output");
    let output_file = File::create(&output).unwrap();
    let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", CHILD_TEST, "--test-threads=1"])
        .env_clear()
        .env(CHILD_READY, &ready)
        .stdout(Stdio::from(output_file.try_clone().unwrap()))
        .stderr(Stdio::from(output_file));
    if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile);
    }
    let mut child = SignalChild(command.spawn().unwrap());
    let status = tokio::time::timeout(Duration::from_secs(5), async {
        while !ready.exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before handler readiness"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            ProcessCommand::new("/bin/kill")
                .args(["-s", signal, &child.0.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        status.is_ok_and(|status| status.success()),
        "child failed to shut down gracefully: {}",
        fs::read_to_string(output).unwrap()
    );
}

#[tokio::test]
async fn unix_shutdown_observes_interrupt_and_termination_in_isolated_children() {
    if let Some(path) = std::env::var_os(CHILD_READY) {
        signal_child(Path::new(&path)).await;
        return;
    }
    assert_signal("INT").await;
    assert_signal("TERM").await;
}
