# Trusted requester snapshot recovery

Purpose: let Arenaton's authenticated gateway recover known RFQs from its own
durable storage after restart, without making the handle deserializable from
arbitrary caller input. No new dependency, credential operation, live request,
CLI command or custody/signing implementation.

- Add a named trusted-storage restoration API taking original CreateRequest,
  original RfqResponse, recorded credential address and current L2Credentials.
- Keep CreatedRfq fields private; do not derive Deserialize for the handle.
- Validate request/credential identity, safe RFQ ID and any returned request's
  matching RFQ/maker/legs/direction/side/size and valid distinct YES/NO positions.
- Permit expired/terminal snapshots for known-ID status recovery; restoration
  never makes a quote executable. Acceptance revalidates real expiry, identities,
  exact order, budget and all existing safety checks before HTTP.
- Expose a minimal pure acceptance preflight if needed so a durable gateway can
  reject an invalid order before reserving its immutable order hash. Reuse the
  exact checks in accept, preserving its second verification before dispatch.
- RED/GREEN tests: successful restore and status; expired restore permits status
  but rejects accept; other account, unsafe ID, mismatched echoed request/positions,
  budget/quote/order rejection before any connection; no handle secret exposure.
- Focused/full execution tests, feature isolation, clippy, fmt and independent
  review before root integration/publication. Preserve source-bound raw logs.

This is a storage recovery mechanism for trusted server records, not proof of
wallet ownership, validation of arbitrary user snapshots, provider authorization,
or production acceptance. The default public feature remains unchanged.

## Verified result

Implemented the exact trusted-storage factory and pure
`CreatedRfq::validate_order(&L2Credentials, &SignedOrder, now_ms)` preflight.
The existing accept checks were moved without weakening them; accept repeats
preflight with actual UTC time immediately before serializing its request.
The handle retains private fields and has no Deserialize implementation.
Binding validation is shared with executable quote validation. Restoration
checks any echoed request even for terminal snapshots, but intentionally does
not require a live quote merely to recover status.

RED: the new tests could not compile against the absent restore/preflight API.
GREEN:21requester tests pass, including five new restore/restart cases,
actual create→snapshot→drop→restore→new client status, Deposit credential
mismatch, unsafe/mixed IDs/positions, expired/terminal recovery, invalid
quote/order/budget preflight and a zero-connection acceptance check after a
historical successful preflight. The full execution suite has204PASS/3ignored
public canaries. Execution clippy all-targets with -Dwarnings, formatting and
diff checks pass. Authenticated-only and no-default feature checks pass;
no-default retains the existing unused first_price warning.

Raw source-bound logs and SHA256 receipt are preserved in ignored
`target/requester-restore-qualification/`. No dependencies, lockfile, CLI,
credentials, live provider calls, custody or signing implementation changed.
Root owns independent review, integration, publication and gateway pinning.
