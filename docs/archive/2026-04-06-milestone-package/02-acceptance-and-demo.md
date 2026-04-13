# Acceptance And Demo

## Package Generation

```bash
python3 eco-concil-runtime/scripts/eco_milestone_package.py --output-dir /home/fpddmw/projects/openclaw-eco-concil_v1/docs/archive/2026-04-06-milestone-package
```

## Recommended Validation Commands

### Route A

Source delivery: `A4` Agent Entry Gate

- `python3 -m unittest tests/test_agent_entry_gate.py -q`
- `python3 -m unittest tests/test_milestone_package.py -q`
- `python3 -m unittest tests/test_progress_dashboard.py -q`
- `python3 eco-concil-runtime/scripts/eco_progress_dashboard.py --pretty`
- `python3 eco-concil-runtime/scripts/eco_milestone_package.py --output-dir docs/archive/2026-04-06-milestone-package --package-date 2026-04-06 --pretty`
- `python3 -m unittest discover -s tests -q`

### Route B

Source delivery: `B3` Moderator Control Consolidation Closeout

- `python3 eco-concil-runtime/scripts/eco_progress_dashboard.py --pretty`
- `python3 -m unittest discover -s tests -q`

### Route C

Source delivery: `C2.2` Non-Python Query Surface

- `python3 -m unittest tests/test_runtime_kernel.py -q`
- `python3 -m unittest tests/test_progress_dashboard.py -q`
- `python3 -m unittest discover -s tests -q`

### Route D

Source delivery: `D4` Milestone / Demo Packaging

- `python3 -m unittest tests/test_milestone_package.py -q`
- `python3 -m unittest tests/test_progress_dashboard.py -q`
- `python3 eco-concil-runtime/scripts/eco_progress_dashboard.py --pretty`
- `python3 eco-concil-runtime/scripts/eco_milestone_package.py --output-dir docs/archive/2026-04-06-milestone-package --package-date 2026-04-06 --pretty`
- `python3 -m unittest discover -s tests -q`

## Demo Walkthrough

```bash
python3 eco-concil-runtime/scripts/eco_progress_dashboard.py --pretty
python3 eco-concil-runtime/scripts/eco_milestone_package.py --output-dir /home/fpddmw/projects/openclaw-eco-concil_v1/docs/archive/2026-04-06-milestone-package --pretty
python3 eco-concil-runtime/scripts/eco_runtime_kernel.py show-run-state --run-dir <run_dir> --round-id <round_id> --pretty
python3 eco-concil-runtime/scripts/eco_runtime_kernel.py list-analysis-result-sets --run-dir <run_dir> --run-id <run_id> --round-id <round_id> --analysis-kind claim-cluster --latest-only --include-contract --pretty
python3 eco-concil-runtime/scripts/eco_runtime_kernel.py query-analysis-result-items --run-dir <run_dir> --run-id <run_id> --round-id <round_id> --analysis-kind claim-cluster --latest-only --subject-id <cluster_id> --include-result-sets --include-contract --pretty
```
