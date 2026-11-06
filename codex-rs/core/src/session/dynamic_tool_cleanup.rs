//! Keep external callback cleanup alive across active-turn removal and task abort.
use std::collections::HashMap;
use std::collections::HashSet;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

const MAX_CALLBACK_IDENTITIES: usize = 16_384;

pub(crate) struct DynamicToolCleanup {
    state: Mutex<CleanupState>,
    limit: usize,
}

#[derive(Default)]
struct CleanupState {
    pending: HashMap<String, PendingCleanup>,
    // A late recovery acknowledgement must never resolve a replacement call
    // with the same origin. Retain used identities for this session's lifetime.
    seen: HashSet<(String, String)>,
}

struct PendingCleanup {
    turn_id: String,
    cancellation: CancellationToken,
    cancelling: bool,
    complete: CancellationToken,
}

impl Default for DynamicToolCleanup {
    fn default() -> Self {
        Self::with_capacity(MAX_CALLBACK_IDENTITIES)
    }
}

impl DynamicToolCleanup {
    fn with_capacity(limit: usize) -> Self {
        Self {
            state: Mutex::new(CleanupState::default()),
            limit,
        }
    }

    pub(crate) async fn register(
        &self,
        turn_id: &str,
        call_id: &str,
        cancellation: CancellationToken,
    ) -> bool {
        let mut state = self.state.lock().await;
        // Never replace an unresolved callback with a reused call identity.
        let identity = (turn_id.to_owned(), call_id.to_owned());
        if cancellation.is_cancelled()
            || state.pending.contains_key(call_id)
            || state.seen.contains(&identity)
            || state.seen.len() >= self.limit
        {
            return false;
        }
        state.seen.insert(identity);
        state.pending.insert(
            call_id.to_owned(),
            PendingCleanup {
                turn_id: turn_id.to_owned(),
                cancellation,
                cancelling: false,
                complete: CancellationToken::new(),
            },
        );
        true
    }

    pub(crate) async fn resolve(
        &self,
        turn_id: Option<&str>,
        call_id: &str,
        cleanup_complete: bool,
    ) -> bool {
        let mut state = self.state.lock().await;
        let Some(entry) = state.pending.get(call_id) else {
            return false;
        };
        if turn_id.is_some_and(|id| id != entry.turn_id)
            || ((entry.cancelling || entry.cancellation.is_cancelled()) && !cleanup_complete)
        {
            return false;
        }
        if let Some(entry) = state.pending.remove(call_id) {
            entry.complete.cancel();
        }
        true
    }

    /// None means no pending entry for this exact origin; accepted recovery
    /// receipts belong to the app-server registry, not this lifecycle barrier.
    pub(crate) async fn status(&self, turn_id: &str, call_id: &str) -> Option<bool> {
        let state = self.state.lock().await;
        state
            .pending
            .get(call_id)
            .filter(|entry| entry.turn_id == turn_id)
            .map(|entry| entry.cancelling || entry.cancellation.is_cancelled())
    }

    pub(crate) async fn cancel_turn_and_wait(
        &self,
        turn_id: &str,
        cancellation: &CancellationToken,
        all_cells: bool,
    ) {
        let pending = {
            let mut state = self.state.lock().await;
            // Serialize cancellation with normal completion. New registrations are
            // already excluded by removal of the active turn before this method.
            cancellation.cancel();
            state
                .pending
                .values_mut()
                .filter_map(|entry| {
                    if !all_cells && entry.turn_id != turn_id {
                        return None;
                    }
                    entry.cancelling = true;
                    entry.cancellation.cancel();
                    Some(entry.complete.clone())
                })
                .collect::<Vec<_>>()
        };
        // A timeout, dropped waiter, disconnect or failed result is not cleanup.
        // Leave teardown pending until the callback owner explicitly acknowledges.
        for complete in pending {
            complete.cancelled().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn cancellation_requires_exact_explicit_acknowledgement() {
        let cleanup = Arc::new(DynamicToolCleanup::default());
        let cancel = CancellationToken::new();
        assert!(cleanup.register("turn", "call", cancel.clone()).await);
        assert!(
            !cleanup
                .register("other", "call", CancellationToken::new())
                .await
        );
        let waiter = tokio::spawn({
            let cleanup = Arc::clone(&cleanup);
            let cancel = cancel.clone();
            async move { cleanup.cancel_turn_and_wait("turn", &cancel, false).await }
        });
        cancel.cancelled().await;
        assert!(!cleanup.resolve(Some("other"), "call", true).await);
        assert!(!cleanup.resolve(Some("turn"), "other", true).await);
        assert!(!cleanup.resolve(Some("turn"), "call", false).await);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        assert!(cleanup.resolve(Some("turn"), "call", true).await);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
        assert!(!cleanup.resolve(Some("turn"), "call", true).await);
    }

    #[tokio::test]
    async fn normal_completion_does_not_need_cancellation_acknowledgement() {
        let cleanup = DynamicToolCleanup::default();
        let cancel = CancellationToken::new();
        assert!(cleanup.register("turn", "call", cancel.clone()).await);
        assert!(cleanup.resolve(Some("turn"), "call", false).await);
        tokio::time::timeout(
            Duration::from_secs(1),
            cleanup.cancel_turn_and_wait("turn", &cancel, false),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn cell_interrupt_waits_for_every_callback_including_an_earlier_turn() {
        let cleanup = Arc::new(DynamicToolCleanup::default());
        let current = CancellationToken::new();
        let earlier = CancellationToken::new();
        assert!(cleanup.register("current", "a", current.clone()).await);
        assert!(cleanup.register("earlier", "b", earlier.clone()).await);
        let waiter = tokio::spawn({
            let cleanup = Arc::clone(&cleanup);
            let current = current.clone();
            async move {
                cleanup
                    .cancel_turn_and_wait("current", &current, true)
                    .await
            }
        });
        earlier.cancelled().await;
        assert!(cleanup.resolve(Some("current"), "a", true).await);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        assert!(!cleanup.resolve(Some("current"), "b", true).await);
        assert!(cleanup.resolve(Some("earlier"), "b", true).await);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn an_already_cancelled_token_still_requires_cleanup_acknowledgement() {
        let cleanup = Arc::new(DynamicToolCleanup::default());
        let cancel = CancellationToken::new();
        assert!(cleanup.register("turn", "call", cancel.clone()).await);
        cancel.cancel();
        assert!(!cleanup.register("turn", "late", cancel.clone()).await);
        let waiter = tokio::spawn({
            let cleanup = Arc::clone(&cleanup);
            async move { cleanup.cancel_turn_and_wait("turn", &cancel, false).await }
        });
        assert!(!cleanup.resolve(Some("turn"), "call", false).await);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        assert!(cleanup.resolve(Some("turn"), "call", true).await);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn accepted_origins_cannot_be_reused_by_a_replacement_callback() {
        let cleanup = DynamicToolCleanup::default();
        assert!(
            cleanup
                .register("first", "call", CancellationToken::new())
                .await
        );
        assert!(cleanup.resolve(Some("first"), "call", true).await);
        assert!(
            !cleanup
                .register("first", "call", CancellationToken::new())
                .await
        );

        // Another turn can use that call ID, but a late acknowledgement from
        // the original turn cannot resolve its new obligation.
        assert!(
            cleanup
                .register("second", "call", CancellationToken::new())
                .await
        );
        assert!(!cleanup.resolve(Some("first"), "call", true).await);
        assert_eq!(cleanup.status("second", "call").await, Some(false));
        assert!(cleanup.resolve(Some("second"), "call", true).await);
    }

    #[tokio::test]
    async fn capacity_retains_pending_obligations_and_resolved_identity_history() {
        let cleanup = DynamicToolCleanup::with_capacity(2);
        assert!(
            cleanup
                .register("turn", "a", CancellationToken::new())
                .await
        );
        assert!(
            cleanup
                .register("turn", "b", CancellationToken::new())
                .await
        );
        assert!(
            !cleanup
                .register("turn", "c", CancellationToken::new())
                .await
        );
        assert_eq!(cleanup.status("turn", "a").await, Some(false));
        assert_eq!(cleanup.status("turn", "b").await, Some(false));
        assert!(cleanup.resolve(Some("turn"), "a", false).await);
        assert!(
            !cleanup
                .register("turn", "a", CancellationToken::new())
                .await
        );
        assert!(
            !cleanup
                .register("turn", "c", CancellationToken::new())
                .await
        );
        assert!(cleanup.resolve(Some("turn"), "b", true).await);
    }

    #[tokio::test]
    async fn cleanup_status_is_bound_to_exact_origin_and_observes_cancellation() {
        let cleanup = DynamicToolCleanup::default();
        let cancellation = CancellationToken::new();
        assert!(cleanup.register("turn", "call", cancellation.clone()).await);
        assert_eq!(cleanup.status("turn", "call").await, Some(false));
        assert_eq!(cleanup.status("other", "call").await, None);
        assert_eq!(cleanup.status("turn", "other").await, None);
        cancellation.cancel();
        assert_eq!(cleanup.status("turn", "call").await, Some(true));
        assert!(!cleanup.resolve(Some("turn"), "call", false).await);
        assert!(cleanup.resolve(Some("turn"), "call", true).await);
        // Absence alone is deliberately not an idempotent acknowledgement.
        assert_eq!(cleanup.status("turn", "call").await, None);
    }

    #[tokio::test]
    async fn verified_completion_in_active_turn_removal_window_releases_cleanup() {
        use crate::state::ActiveTurn;
        use codex_protocol::dynamic_tools::DynamicToolResponse;

        let (session, _, _) = crate::session::tests::make_session_and_context_with_rx().await;
        let active = ActiveTurn::default();
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        active.turn_state.lock().await.insert_pending_dynamic_tool(
            "call".to_owned(),
            "turn".to_owned(),
            tx,
        );
        *session.active_turn.lock().await = Some(active);
        let cancellation = CancellationToken::new();
        assert!(
            session
                .dynamic_tool_cleanup
                .register("turn", "call", cancellation.clone())
                .await
        );
        // Reproduce take_active_turn -> handle_task_abort ordering exactly:
        // the sender is retained by teardown, but active_turn is already empty.
        let removed = session.active_turn.lock().await.take();
        assert!(!cancellation.is_cancelled());
        session
            .notify_dynamic_tool_response_for_turn(
                Some("turn"),
                "call",
                DynamicToolResponse {
                    content_items: Vec::new(),
                    success: true,
                },
                true,
            )
            .await;
        assert_eq!(
            session.dynamic_tool_cleanup.status("turn", "call").await,
            None
        );
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        tokio::time::timeout(
            Duration::from_secs(1),
            session
                .dynamic_tool_cleanup
                .cancel_turn_and_wait("turn", &cancellation, false),
        )
        .await
        .expect("verified normal cleanup precedes cancellation teardown");
        drop(removed);
    }
}
