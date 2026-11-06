//! Dynamic-tool handlers finish their item lifecycle when cancellation is signalled.

use super::*;
use crate::state::ActiveTurn;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

async fn next_item(receiver: &async_channel::Receiver<Event>) -> DynamicToolCallItem {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let item = match receiver.recv().await.expect("event channel open").msg {
                EventMsg::ItemStarted(event) => event.item,
                EventMsg::ItemCompleted(event) => event.item,
                _ => continue,
            };
            if let TurnItem::DynamicToolCall(item) = item {
                return item;
            }
        }
    })
    .await
    .expect("dynamic tool item should be emitted")
}

#[tokio::test]
async fn response_cancel_race_completes_once() {
    let (session, turn, receiver) = crate::session::tests::make_session_and_context_with_rx().await;
    *session.active_turn.lock().await = Some(ActiveTurn::default());
    let cancellation = CancellationToken::new();
    let lifecycle = tokio::spawn({
        let session = Arc::clone(&session);
        let cancellation = cancellation.clone();
        async move {
            request_dynamic_tool(
                &session,
                &turn,
                "call-1".to_string(),
                ToolName::plain("gate"),
                json!({}),
                cancellation,
            )
            .await
        }
    });
    next_item(&receiver).await;
    let response = DynamicToolResponse {
        content_items: Vec::new(),
        success: true,
    };
    session
        .notify_dynamic_tool_response("call-1", response.clone())
        .await;
    cancellation.cancel();
    let result = lifecycle.await.expect("lifecycle joined");
    assert!(result.is_none() || result == Some(response));
    let item = next_item(&receiver).await;
    assert_eq!(
        (item.status, item.success, item.error),
        if result.is_some() {
            (DynamicToolCallStatus::Completed, Some(true), None)
        } else {
            (
                DynamicToolCallStatus::Failed,
                Some(false),
                Some("dynamic tool call was cancelled before receiving a response".to_string()),
            )
        }
    );
    while let Ok(event) = receiver.try_recv() {
        assert!(!matches!(event.msg, EventMsg::ItemCompleted(_)));
    }
}

#[tokio::test]
async fn scoped_callback_response_preserves_replacement_and_accepts_recorded_origin() {
    let (session, _, _) = crate::session::tests::make_session_and_context_with_rx().await;
    let active = ActiveTurn::default();
    let (tx, mut rx) = oneshot::channel();
    active.turn_state.lock().await.insert_pending_dynamic_tool(
        "reused".into(),
        "new-origin".into(),
        tx,
    );
    *session.active_turn.lock().await = Some(active);
    let response = DynamicToolResponse {
        content_items: Vec::new(),
        success: true,
    };
    session
        .notify_dynamic_tool_response_for_turn(Some("old-origin"), "reused", response.clone(), true)
        .await;
    assert!(matches!(
        rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    // No current task ID is needed: the pending call owns its original identity.
    session
        .notify_dynamic_tool_response_for_turn(
            Some("new-origin"),
            "reused",
            response.clone(),
            false,
        )
        .await;
    assert_eq!(rx.await.unwrap(), response);
}
