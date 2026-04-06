# Risk Register

当前风险来自各路线最近一次交付中仍然保留的 `Known limitations`，因此它们代表的是“当前残余限制”，而不是更早已被后续阶段解决的问题。

| Route | Source Delivery | Residual Risk |
| --- | --- | --- |
| `A` Runtime / Governance Stabilization | `A3` Governance Regression Hardening | Benchmark/replay still treat `board_summary / board_brief` and the reporting/final-publication family as artifact-shaped outputs rather than plane-backed objects. |
| `A` Runtime / Governance Stabilization | `A3` Governance Regression Hardening | Scenario replay still preserves the original `run_id / round_id` identity contract and does not yet support remapping one frozen fixture onto a different run/round namespace. |
| `B` Deliberation Plane / Moderator Loop | `B3` Moderator Control Consolidation Closeout | `promotion_freezes`, `moderator_action_snapshots`, `falsification_probe_snapshots`, and `round_task_snapshots` remain latest-snapshot recovery surfaces rather than full per-item history logs. |
| `B` Deliberation Plane / Moderator Loop | `B3` Moderator Control Consolidation Closeout | Some runtime lineage discovery still uses runtime/artifact footprints; `B3` closes the operational moderator control path, but it does not introduce a wholly new DB-native round-discovery model. |
| `C` Analysis Plane / DB-First Analysis | `C2.2` Non-Python Query Surface | The current query surface is intentionally filtered JSON output, not a generic SQL or ad hoc expression language; callers still need to compose more complex joins client-side. |
| `C` Analysis Plane / DB-First Analysis | `C2.2` Non-Python Query Surface | The runtime CLI currently exposes analysis-plane reads only; there is still no corresponding formal non-Python write or mutation surface for analysis objects. |
| `D` Program Control / Documentation | `D4` Milestone / Demo Packaging | The milestone package is still a control/documentation snapshot generated from plan/log/dashboard inputs plus latest route-delivery notes; it does not embed a runnable runtime fixture, DB snapshot, or large artifact archive. |
| `D` Program Control / Documentation | `D4` Milestone / Demo Packaging | The acceptance/demo walkthrough intentionally stays at command-book level; operators still need to supply concrete `<run_dir> / <run_id> / <round_id>` values when demonstrating live runtime/query surfaces. |
