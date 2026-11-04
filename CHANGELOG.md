# Changelog

## v2.0.0 — 2026-10-23

Rewritten to sit on the host's own service contracts instead of reaching into
internals, which is what made the 1.x build brittle across harness revisions.

- **Boot timing attribution** — where a profile actually spends its startup
  milliseconds, per bundle.
- **Tool-call latency sampling** — P50/P95/P99 per tool name, so a slow plugin is
  identifiable instead of guessed at.
- **Session token attribution** — which session and which turn consumed the budget.
- **Cross-session memory gap diagnosis** — shows what a session could not see,
  which is the failure mode users report as "it forgot".
- CoreProtect-style rewind over the recorded event stream.
- `test/smoke.mjs` drives the plugin with a fake `ctx` — no harness boot needed —
  and asserts the register / command / sampling / HTTP-route contract.
- `test/edge.mjs` covers empty sessions and a missing tools service.
