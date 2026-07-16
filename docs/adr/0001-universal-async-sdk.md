# ADR-0001: Universal Async SDK with Safe Public Default

Status: accepted
Date: 2026-07-16

## Context

Polyrover's canonical MegaBot consumer is Tokio-based, while the crate used
blocking reqwest and Tungstenite behind thread bridges. The crate also retained
authenticated, wallet, execution DTO, and bridge DTO surfaces despite being
described as read-only.

## Decision

Polyrover is a universal standalone Rust interface to Polymarket. Network APIs
are async-only. Pure DTOs, parsing, simulation, HMAC helpers, wallet derivation,
and book math remain synchronous.

Cargo features are layered as `public` (default), `authenticated`, `wallet`,
`execution`, `bridge`, and `full`. Features limit compilation and dependency
exposure; they are not authorization controls.

Core market and outcome identities are generic. Crypto Up/Down discovery is a
specialized helper. Strategy, sizing, and portfolio policy stay outside the SDK.

This migration preserves existing behavior and adds no live order, cancellation,
private-key, relayer, or bridge execution method.

## Consequences

Pre-1.0 callers must await network methods. No blocking facade is maintained.
MegaBot compiles only `public`; Polygolem remains MegaBot's exclusive signing
and execution boundary. Any future fund-moving capability requires a separate
safety design and architecture approval.
