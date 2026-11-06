# Sovereign callback cleanup acknowledgement

This protocol is implemented by the explicit Sovereign read-head owner mode.
The recovery branch contains its server, callback client and CLI/TUI integration;
normal sessions retain their existing behavior. Source recovery does not install
or select a runtime. See [recovery provenance](witnessops-recovery.md).

When an in-flight dynamic tool receives its callback-specific failed
`item/completed` cancellation notification, its external owner must stop its
effects, terminate and reap child processes, and finish cleanup. Only then may
it answer the original `item/tool/call` JSON-RPC request:

```json
{
  "id": "the original server request ID",
  "result": {
    "contentItems": [],
    "success": false,
    "cleanupComplete": true
  }
}
```

The response is bound to the original request, turn and call. Scoped callbacks
accept responses only from their original recipient connections. The cleanup
barrier survives removal of the active turn. Its response path does not wait
on the operation queue that interruption is blocking, and cannot deliver a late
result to a replacement turn. Interrupting all Code Mode cells also waits for
pending callbacks whose cells originated in earlier turns.

No elapsed-time limit, failed result, malformed response, missing/false
acknowledgement, or closed transport is evidence of cleanup. These leave
teardown pending. An acknowledgement is the external owner's assertion;
it is not independent proof of effects on an arbitrary external system.
The offline tests separately inspect real process identities, pidfds and reaping.

Normal responses before cancellation retain their existing completion behavior.
Existing client implementations default the new field to false; they do not
automatically gain authority to assert cleanup. The integrated client retains its owner across a transport disconnect and can
retry cleanup status/acknowledgement against the same server generation without
replaying the callback. This recovery is in memory: server/client process-crash
recovery and durable outboxes are not implemented. Runtime selection and
operational acceptance remain separate from source recovery and offline tests.

The callback regressions exercise ordinary and detached-session grandchildren,
delay acknowledgement beyond the graceful interruption timeout, reject wrong
turn/call identities and a foreign connection, and sample process exit/reaping
at the internal abort hook as well as public interrupt and completion messages.
