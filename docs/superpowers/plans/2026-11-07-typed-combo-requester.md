# Typed Combo requester: local SDK contract

Goal: a server-side authenticated RFQ client whose create/accept/status operations use the published requester protocol, preserve uncertain outcomes and never misrepresent a failed business result as an executable quote. The public default and CLI remain unchanged; no credentials, wallet signer, gateway deployment or financial acceptance is supplied by this implementation.

Source: official requester guide https://docs.polymarket.com/trading/combos/requesters and builder distinction https://docs.polymarket.com/trading/combos/builders, consulted 2026-11-07. Requester uses `/v1/requester/rfq/requests`, returned millisecond expiry, positions rather than CLOB token IDs, zero builder attribution, existing CLOB L2 HMAC. Builder onboarding and identity/custody are separate. No requester cancellation route is inferred.

Design: expose the module only under the existing `execution` feature (which includes authenticated). This preserves the authenticated-only read contract; requester create/accept are mutative. Reuse L2 header construction after strict base64-secret validation. Keep the shared public transport unchanged: its redirects, unbounded bodies and retry behavior are unsuitable for this protocol. Use the existing reqwest dependency in a module-local no-redirect/no-proxy client restricted to the fixed official host or an explicit literal-loopback test constructor. All requests have a total deadline and bounded response; no operation retries automatically. Transport failures after create have an unknown outcome, including response truncation/invalid JSON; known RFQ IDs are retained for acceptance/status recovery. Upstream bodies and credential/header values never appear in errors.

Typed models cover account identities, direction/size, returned request/quote, signed Exchange v3 order and future status/error strings. Quote acceptance validates account, returned RFQ/request/quote bindings, exact selected legs/size, received expiry and signed-order identity/amount/token/zero-builder fields. Signature generation, cryptographic signature recovery and market-leg compatibility proof remain caller/provider responsibilities, explicitly documented. HTTP200/FAILED and unknown states never grant an executable quote.

Validation: regression tests first, local loopback HTTP only; cover exact body/HMAC/path, proxy/deposit identities, validation/no-write, expired/mixed quotes, business failure/future status, signed-order mismatch, create timeout without retry, redirect refusal, bounded/redacted response and status recovery. Then formatting, authenticated focused/full tests, public/no-default feature checks and clippy. Root reviews and integrates; DEBT031 remains open for trusted account mapping, app gateway and actual funded acceptance.

## Verification receipt

- Before implementation: new contract test failed to compile because `combo_requester` did not exist (preserved RED log); no pre-existing runtime behavior is misrepresented as a regression.
- Final focused requester contract:12tests PASS, all local HTTP/pure validation.
- Full `cargo test --features execution`:195PASS,3explicit ignored live tests,14suites; no provider write or credential operation.
- `cargo check --no-default-features`:PASS with the pre-existing unused `first_price` warning in models/types.rs. `cargo check --no-default-features --features authenticated`:PASS.
- `cargo clippy --all-targets --features execution -- -D warnings`:PASS. Formatting/diff check:PASS.
- Raw logs and SHA256 receipt: ignored `target/requester-qualification/`; root must preserve these before retiring the worktree. No dependency/lock change.

Stop condition reached: callable SDK requester contract and local guards are implemented and verified. Full DEBT031 cannot close from this evidence: trusted user/session-to-wallet mapping, gateway wiring, signing authority, deployment and authorized funded acceptance remain separate.

## Review correction: maker spend bound

The independent review found that checking only total_required against the requested budget still permitted a larger signed makerAmount. Four runtime regressions reproduced the gap: BUY collateral and SELL shares, both direct quote validation and actual unintended acceptance connections. The guard now requires maker_amount <= total_required <= requested_size, in the relevant spend units. The same tests pass with no acceptance connection; no speculative fee/net-receive formula was added. Raw RED/GREEN/full/clippy follow-up evidence is preserved separately.
