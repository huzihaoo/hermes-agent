# G1Q3 RCA release scorecard (W0)

`scripts/pnc_rca_release_scorecard.py` is a read-only snapshot of the current
PNC RCA release. Its output is always labeled `NOT_GA`; the scorecard cannot
authorize a release or override any GA gate.

## Run

```bash
python3 scripts/pnc_rca_release_scorecard.py > scorecard.json
python3 scripts/pnc_rca_release_scorecard.py --format markdown > scorecard.md
python3 scripts/pnc_rca_release_scorecard.py --validate scorecard.json
```

The command reads the canonical live manifest, active release binding,
resident health/state files, deployed business profile registry, and the RCA
control database. SQLite is opened with URI `mode=ro`, `PRAGMA query_only=ON`,
and a read transaction. The command does not restart services, mutate a plist,
write a database, create a trigger, publish an effect, or call a network API.

## Source boundaries

- `live`: current active host/pipeline/worker/mcap fingerprints, resident loaded
  paths, deployed profile readiness, activation ledger state, tier projection,
  requester identity denominators, and current-release canary state.
- `reference`: the GA contract used to interpret the live evidence.
- `historical`: archived activation receipts/manifests and the latest observed
  71-ticket classification ledger.

Historical `evidence_attribution` is preserved as a reported label. It is known
to contain classification conflicts and is not presented as current
high-confidence truth. W1 owns the oracle recomputation.

Requester identities are split as `human` (`ou_*`), `automation`
(`automation:*`), `legacy_automation` (`operator-`, `operator_`, `codex-`, or
`codex_`), and `unknown`. When W10 is integrated, this projection should
delegate to `gateway.pnc_rca_requester_identity.classify_rca_requester`.

## Failure behavior

Missing or empty fingerprints, profile readiness, live control rows,
provenance, lineage fields, or historical counts cause exit code `2`. A real
zero value remains visible where zero is a valid observation. A canary that has
not run for the active binding is reported as
`not_observed_for_active_release`; an older canary is retained only under
`latest_observation` and cannot make the current release green.
