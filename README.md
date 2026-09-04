# Robinhood SPY Maker/Taker Experiment

Safety-focused experimental runner for placing post-only SPY maker quotes and flattening fills with a taker hedge.

## Contents

- `spy_maker_taker_experiment.py`: experiment state machine and exchange adapter
- `tests/test_spy_maker_taker_experiment.py`: unit and safety tests
- `tools/experiment_report.py`: offline experiment report generator

## Safety

The runner defaults to configuration validation/read-only preflight when dry-run mode is enabled. Live execution requires explicit environment configuration. Never commit credentials, account configuration, state, fills, or experiment logs.

This repository contains execution research code. Review exchange rules and campaign terms before use. Maker volume does not earn Trading Points in Aster USD1 RWA Boost Phase 1.

## Test

```bash
python3 -m unittest tests/test_spy_maker_taker_experiment.py
```

One process-discovery test relies on Linux `/proc` behavior and is intended to run on the deployment host.
