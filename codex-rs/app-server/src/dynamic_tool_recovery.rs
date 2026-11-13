//! Cleanup-only recovery for callbacks whose execution must never be replayed.
//!
//! Records and accepted tombstones live for this app-server instance. Admission
//! is bounded; an unresolved obligation is never evicted to make room.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use codex_app_server_protocol::DynamicToolCleanupIdentity;
use codex_app_server_protocol::RequestId;
use codex_app_server_transport::ConnectionId;
use codex_core::CodexThread;
use sha2::Digest;
use sha2::Sha256;
use tokio::sync::Mutex;
use uuid::Uuid;

// Three canonical UUIDs and two separators. Reject oversized capabilities
// before hashing them; tokens are opaque to their recipients.
const CAPABILITY_LENGTH: usize = 110;

/// This type deliberately has no Debug implementation: a recovery capability
/// must only leave this registry through the initial register result.
pub(crate) struct DynamicToolRecovery {
    instance: Uuid,
    limit: usize,
    state: Mutex<RecoveryState>,
}

#[derive(Default)]
struct RecoveryState {
    records: HashMap<RequestId, RecoveryRecord>,
    closed_connections: HashSet<ConnectionId>,
    closed_history_full: bool,
}

struct RecoveryRecord {
    thread_id: String,
    turn_id: String,
    call_id: String,
    capability_digest: [u8; 32],
    owner: ConnectionId,
    owner_detached: bool,
    acknowledged: bool,
    // Resolve the exact runtime instance which emitted the callback. A cold
    // reload can replace thread_manager's object under the same thread ID.
    original_thread: Option<Arc<CodexThread>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RecoverySnapshot {
    pub(crate) owner: ConnectionId,
    pub(crate) owner_detached: bool,
    pub(crate) acknowledged: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecoveryError {
    AdmissionFull,
    DuplicateRequest,
    OwnerClosed,
    InvalidIdentity,
    OriginalThreadUnavailable,
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AdmissionFull => "dynamic tool cleanup recovery capacity exhausted",
            Self::DuplicateRequest => "dynamic tool cleanup request identity is already registered",
            Self::OwnerClosed => "dynamic tool callback owner is disconnected",
            Self::InvalidIdentity => "unknown dynamic tool cleanup identity",
            Self::OriginalThreadUnavailable => "original callback runtime instance is unavailable",
        })
    }
}

impl std::error::Error for RecoveryError {}

impl Default for DynamicToolRecovery {
    fn default() -> Self {
        Self::with_capacity(crate::DYNAMIC_TOOL_RECOVERY_RECORD_LIMIT)
    }
}

impl DynamicToolRecovery {
    fn with_capacity(limit: usize) -> Self {
        Self {
            instance: Uuid::new_v4(),
            limit,
            state: Mutex::new(RecoveryState::default()),
        }
    }

    /// Register exactly one executing owner before dispatch. The opaque token
    /// authorizes only cleanup/status for this exact identity, not execution.
    #[cfg(test)]
    pub(crate) async fn register(
        &self,
        request_id: RequestId,
        thread_id: &str,
        turn_id: &str,
        call_id: &str,
        owner: ConnectionId,
    ) -> Result<String, RecoveryError> {
        self.register_with_origin(request_id, thread_id, turn_id, call_id, owner, None)
            .await
    }

    /// Production dispatch supplies the original conversation before sending
    /// the request. A later manager lookup by ThreadId is not equivalent.
    pub(crate) async fn register_with_origin(
        &self,
        request_id: RequestId,
        thread_id: &str,
        turn_id: &str,
        call_id: &str,
        owner: ConnectionId,
        original_thread: Option<Arc<CodexThread>>,
    ) -> Result<String, RecoveryError> {
        let mut state = self.state.lock().await;
        if state.closed_connections.contains(&owner) {
            return Err(RecoveryError::OwnerClosed);
        }
        if state.closed_history_full || state.records.len() >= self.limit {
            return Err(RecoveryError::AdmissionFull);
        }
        if state.records.contains_key(&request_id) {
            return Err(RecoveryError::DuplicateRequest);
        }
        // UUID v4 uses the platform randomness source. Two independent random
        // nonces keep the per-callback capability unguessable even when another
        // callback from this server instance has disclosed its instance prefix.
        let capability = format!("{}.{}.{}", self.instance, Uuid::new_v4(), Uuid::new_v4());
        state.records.insert(
            request_id,
            RecoveryRecord {
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                call_id: call_id.to_owned(),
                capability_digest: capability_digest(&capability),
                owner,
                owner_detached: false,
                acknowledged: false,
                original_thread,
            },
        );
        Ok(capability)
    }

    /// Read a cleanup obligation without admitting the caller as an executor.
    pub(crate) async fn lookup(
        &self,
        identity: &DynamicToolCleanupIdentity,
    ) -> Result<RecoverySnapshot, RecoveryError> {
        let state = self.state.lock().await;
        let record = state
            .records
            .get(&identity.request_id)
            .filter(|record| record.matches(identity))
            .ok_or(RecoveryError::InvalidIdentity)?;
        Ok(record.snapshot())
    }

    /// Atomically distinguish an accepted tombstone from a pending obligation
    /// and pin the original runtime. None means accepted, never merely absent.
    /// This avoids racing receipt acceptance against dropping its runtime pin.
    pub(crate) async fn pending_original(
        &self,
        identity: &DynamicToolCleanupIdentity,
    ) -> Result<Option<Arc<CodexThread>>, RecoveryError> {
        let state = self.state.lock().await;
        let record = state
            .records
            .get(&identity.request_id)
            .filter(|record| record.matches(identity))
            .ok_or(RecoveryError::InvalidIdentity)?;
        if record.acknowledged {
            return Ok(None);
        }
        record
            .original_thread
            .clone()
            .map(Some)
            .ok_or(RecoveryError::OriginalThreadUnavailable)
    }

    /// Disconnect records uncertainty; it never supplies cleanup evidence.
    /// Remember closure even before registration to close that ordering race.
    pub(crate) async fn mark_detached(&self, owner: ConnectionId) {
        let mut state = self.state.lock().await;
        for record in state.records.values_mut() {
            if record.owner == owner {
                record.owner_detached = true;
            }
        }
        if !state.closed_connections.contains(&owner) {
            if state.closed_connections.len() < self.limit {
                state.closed_connections.insert(owner);
            } else {
                // Keep memory bounded without forgetting a closed owner and
                // admitting a late callback for it. Existing recovery remains usable.
                state.closed_history_full = true;
            }
        }
    }

    /// Record acceptance only after the caller has delivered verified cleanup
    /// to core. True means this exact acknowledgement was already accepted.
    /// The tombstone remains available after the owning connection is gone.
    pub(crate) async fn acknowledge(
        &self,
        identity: &DynamicToolCleanupIdentity,
    ) -> Result<bool, RecoveryError> {
        let mut state = self.state.lock().await;
        let record = state
            .records
            .get_mut(&identity.request_id)
            .filter(|record| record.matches(identity))
            .ok_or(RecoveryError::InvalidIdentity)?;
        let already_acknowledged = record.acknowledged;
        record.acknowledged = true;
        // Accepted tombstones need identity/receipt state, not a runtime pin.
        record.original_thread.take();
        Ok(already_acknowledged)
    }
}

impl RecoveryRecord {
    fn matches(&self, identity: &DynamicToolCleanupIdentity) -> bool {
        self.thread_id == identity.thread_id
            && self.turn_id == identity.turn_id
            && self.call_id == identity.call_id
            && identity.capability.len() == CAPABILITY_LENGTH
            && self.capability_digest == capability_digest(&identity.capability)
    }

    fn snapshot(&self) -> RecoverySnapshot {
        RecoverySnapshot {
            owner: self.owner,
            owner_detached: self.owner_detached,
            acknowledged: self.acknowledged,
        }
    }
}

fn capability_digest(capability: &str) -> [u8; 32] {
    Sha256::digest(capability.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn register(
        recovery: &DynamicToolRecovery,
        request_id: i64,
        owner: ConnectionId,
    ) -> DynamicToolCleanupIdentity {
        let request_id = RequestId::Integer(request_id);
        let capability = recovery
            .register(request_id.clone(), "thread", "turn", "call", owner)
            .await
            .expect("register callback");
        DynamicToolCleanupIdentity {
            thread_id: "thread".to_owned(),
            turn_id: "turn".to_owned(),
            call_id: "call".to_owned(),
            request_id,
            capability,
        }
    }

    #[tokio::test]
    async fn every_identity_component_and_capability_must_match() {
        let recovery = DynamicToolRecovery::default();
        let identity = register(&recovery, 1, ConnectionId(1)).await;
        let other = register(&recovery, 2, ConnectionId(2)).await;
        let mut wrong = Vec::new();
        for field in [
            "thread",
            "turn",
            "call",
            "request",
            "capability",
            "oversized",
        ] {
            let mut candidate = identity.clone();
            match field {
                "thread" => candidate.thread_id.push('x'),
                "turn" => candidate.turn_id.push('x'),
                "call" => candidate.call_id.push('x'),
                "request" => candidate.request_id = RequestId::String("1".to_owned()),
                "capability" => candidate.capability = other.capability.clone(),
                "oversized" => candidate.capability.push('x'),
                _ => unreachable!(),
            }
            wrong.push(candidate);
        }
        for candidate in wrong {
            assert_eq!(
                recovery.lookup(&candidate).await,
                Err(RecoveryError::InvalidIdentity)
            );
            assert_eq!(
                recovery.acknowledge(&candidate).await,
                Err(RecoveryError::InvalidIdentity)
            );
        }
        assert!(!recovery.lookup(&identity).await.unwrap().acknowledged);
        assert!(!recovery.lookup(&other).await.unwrap().acknowledged);
    }

    #[tokio::test]
    async fn exact_acknowledgement_retries_are_idempotent_after_disconnect() {
        let recovery = DynamicToolRecovery::default();
        let identity = register(&recovery, 1, ConnectionId(1)).await;
        recovery.mark_detached(ConnectionId(1)).await;
        assert_eq!(
            recovery.lookup(&identity).await.unwrap(),
            RecoverySnapshot {
                owner: ConnectionId(1),
                owner_detached: true,
                acknowledged: false,
            }
        );
        assert!(!recovery.acknowledge(&identity).await.unwrap());
        assert!(recovery.acknowledge(&identity).await.unwrap());
        let state = recovery.lookup(&identity).await.unwrap();
        assert!(state.acknowledged && state.owner_detached);
    }

    #[tokio::test]
    async fn disconnect_only_detaches_its_original_callbacks() {
        let recovery = DynamicToolRecovery::default();
        let first = register(&recovery, 1, ConnectionId(1)).await;
        let second = register(&recovery, 2, ConnectionId(2)).await;
        recovery.mark_detached(ConnectionId(1)).await;
        let first = recovery.lookup(&first).await.unwrap();
        let second = recovery.lookup(&second).await.unwrap();
        assert!(first.owner_detached && !first.acknowledged);
        assert!(!second.owner_detached && !second.acknowledged);
    }

    #[tokio::test]
    async fn disconnect_before_registration_cannot_admit_a_late_callback() {
        let recovery = DynamicToolRecovery::default();
        recovery.mark_detached(ConnectionId(1)).await;
        assert_eq!(
            recovery
                .register(
                    RequestId::Integer(1),
                    "thread",
                    "turn",
                    "call",
                    ConnectionId(1)
                )
                .await,
            Err(RecoveryError::OwnerClosed)
        );
        register(&recovery, 2, ConnectionId(2)).await;
    }

    #[tokio::test]
    async fn capacity_never_evicts_pending_records_or_accepted_tombstones() {
        let recovery = DynamicToolRecovery::with_capacity(2);
        let pending = register(&recovery, 1, ConnectionId(1)).await;
        let accepted = register(&recovery, 2, ConnectionId(2)).await;
        assert!(!recovery.acknowledge(&accepted).await.unwrap());
        assert_eq!(
            recovery
                .register(
                    RequestId::Integer(3),
                    "thread",
                    "turn",
                    "other",
                    ConnectionId(3)
                )
                .await,
            Err(RecoveryError::AdmissionFull)
        );
        assert!(!recovery.lookup(&pending).await.unwrap().acknowledged);
        assert!(recovery.acknowledge(&accepted).await.unwrap());
        assert!(!recovery.acknowledge(&pending).await.unwrap());
    }

    #[tokio::test]
    async fn request_ids_cannot_be_replaced_after_acknowledgement() {
        let recovery = DynamicToolRecovery::default();
        let identity = register(&recovery, 1, ConnectionId(1)).await;
        recovery.acknowledge(&identity).await.unwrap();
        assert_eq!(
            recovery
                .register(
                    RequestId::Integer(1),
                    "other",
                    "other",
                    "other",
                    ConnectionId(2)
                )
                .await,
            Err(RecoveryError::DuplicateRequest)
        );
        assert!(recovery.acknowledge(&identity).await.unwrap());
    }

    #[tokio::test]
    async fn capability_cannot_cross_server_instances_or_appear_in_snapshots() {
        let first = DynamicToolRecovery::default();
        let second = DynamicToolRecovery::default();
        let original = register(&first, 1, ConnectionId(1)).await;
        let replacement = register(&second, 1, ConnectionId(1)).await;
        assert_ne!(original.capability, replacement.capability);
        assert_eq!(
            second.acknowledge(&original).await,
            Err(RecoveryError::InvalidIdentity)
        );
        let snapshot = first.lookup(&original).await.unwrap();
        assert!(!format!("{snapshot:?}").contains(&original.capability));
        assert!(
            !RecoveryError::InvalidIdentity
                .to_string()
                .contains(&original.capability)
        );
        assert!(!second.lookup(&replacement).await.unwrap().acknowledged);
    }

    #[tokio::test]
    async fn exhausted_closed_owner_history_preserves_existing_recovery() {
        let recovery = DynamicToolRecovery::with_capacity(2);
        let identity = register(&recovery, 1, ConnectionId(1)).await;
        for id in [ConnectionId(1), ConnectionId(2), ConnectionId(3)] {
            recovery.mark_detached(id).await;
        }
        assert_eq!(
            recovery
                .register(
                    RequestId::Integer(2),
                    "thread",
                    "turn",
                    "new",
                    ConnectionId(4)
                )
                .await,
            Err(RecoveryError::AdmissionFull)
        );
        assert!(!recovery.acknowledge(&identity).await.unwrap());
        let state = recovery.state.lock().await;
        assert_eq!(state.closed_connections.len(), 2);
        assert_eq!(state.records.len(), 1);
    }

    #[tokio::test]
    async fn missing_original_instance_is_not_resolved_from_current_thread_id() {
        let recovery = DynamicToolRecovery::default();
        let identity = register(&recovery, 1, ConnectionId(1)).await;
        assert!(matches!(
            recovery.pending_original(&identity).await,
            Err(RecoveryError::OriginalThreadUnavailable)
        ));
        let mut wrong = identity.clone();
        wrong.capability.push('x');
        assert!(matches!(
            recovery.pending_original(&wrong).await,
            Err(RecoveryError::InvalidIdentity)
        ));
        // Absence of a runtime pin is not itself acceptance or cleanup proof.
        assert!(!recovery.lookup(&identity).await.unwrap().acknowledged);
    }

    #[tokio::test]
    async fn pending_original_only_returns_none_for_an_exact_accepted_receipt() {
        let recovery = DynamicToolRecovery::default();
        let identity = register(&recovery, 1, ConnectionId(1)).await;
        assert!(matches!(
            recovery.pending_original(&identity).await,
            Err(RecoveryError::OriginalThreadUnavailable)
        ));
        recovery.acknowledge(&identity).await.unwrap();
        assert!(matches!(
            recovery.pending_original(&identity).await,
            Ok(None)
        ));
        let mut wrong = identity.clone();
        wrong.turn_id.push('x');
        assert!(matches!(
            recovery.pending_original(&wrong).await,
            Err(RecoveryError::InvalidIdentity)
        ));
    }
}
