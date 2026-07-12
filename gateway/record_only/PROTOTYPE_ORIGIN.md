# Unified record-only outbound prototype

This is a sandbox-only, provisional sink for candidate gateway
sends/replies/updates, relay text, and Feishu card create/update paths. It
imports no platform SDK and contains no network client. Its machine-readable
status is fixed to:

- `provisional_target_only=true`
- `production_ready=false`
- `promotion_authorized=false`
- `candidate_execution_authorized=false`
- `cutover_authorized=false`
- `record_only_coverage_complete=false`
- `external_delivery_attempted=false`
- `external_delivery_verified=false`
- `caller_claims_verified=false`
- `success_scope=record_persisted_not_delivered`

An adapter result with `success=true` means only that a simulated record was
persisted. It never means a message/card was delivered or updated. Card update
responses return `updated=false` plus `simulated_update_recorded=true`, and all
returned message IDs are synthetic.

## Authoritative static census binding

`census_binding.py` fail-closes while importing the transport unless it can
securely read and validate the target-only evidence work queue:

- Machine index: `../evidence/target-outbound-census/INDEX.json`
- INDEX SHA-256: `b6bcfb3a597da616bec2acc8e57eea18695b0bb20e29446926cf2eb2e3f81914`
- Canonical artifact: `census-v4.json`
- Artifact SHA-256: `d2c17c7b03642074d301259437f17cc879e8adfbd91d07029c2dda775a563e63`
- Source commit: `9de9c25f620ff7f1ce0fd5457d596052d5159596`
- Source tree: `1624297419fab639f57302244f6bb28b161bd014`
- Source file-manifest SHA-256: `8df101b0f85845864b3956266b3c4a07412ad44b4cac05acea9d8a265a4c6dbe`
- Tree inventory SHA-256: `320341a68141ee65be7e25463ef27370d83ba2a0f006fafe5c19239ef69c6c0f`
- Status/gate: `PROVISIONAL_STATIC_OUTBOUND_CENSUS_NO_GO` / `NO_GO`

The verifier opens one canonical evidence directory descriptor and uses
`openat`-style, no-follow reads. It rejects a non-canonical root, traversal,
symlink/hardlink inputs, unsafe owner/mode/type/size, path replacement, in-read
mutation, duplicate JSON keys, INDEX/artifact digest mismatch, unexpected
fields, provenance/count/scanner mismatch, and any true authorization gate.
`census-v1.json`, `census-v2.json`, `census-v3.json`, and
`census-v3.repro.json` are explicitly superseded and cannot be selected by the
machine INDEX. In particular, the old v2 digest
`5954ed1976c85b27c473e294514b3ea6dd021dbcc5ae9d1a8ae0f35d23bb5c94`
is a negative test fixture only and is never an accepted binding.

The verifier also recomputes row-language, runtime-category, runtime/test, and
pending/unverified counts from all rows. The bound work queue has 6,338 rows,
including 3,612 runtime/non-test rows and 2,726 test rows. All 6,338 remain
`pending` and `unverified`; none is classified by this prototype.

Five executable-mode anomalies remain open:

- `.github/pr-screenshots/39327/providers-collapsed.png`
- `.github/pr-screenshots/39327/providers-expanded.png`
- `.github/pr-screenshots/39327/tools-collapsed.png`
- `.github/pr-screenshots/39327/tools-expanded.png`
- `optional-skills/devops/docker-management/SKILL.md`

Dynamic import/reflection coverage, skill/plugin coverage, tool-driven
subprocess-descendant coverage, and runtime egress tracing all remain false.
The source-manifest digests above are bound as census provenance fields; this
prototype does not open or independently attest the source manifests.

## Ledger controls

The v2 ledger is an atomically replaced, fsynced authenticated JSONL document.
Its header authenticates generation, row count, chain head, every No-Go safety
gate, and the complete scalar INDEX/canonical/source/scanner/count binding.
Every row repeats that binding and has an HMAC plus previous-row HMAC. Strict
parsing rejects duplicate JSON keys, non-canonical encoding, mutation,
interior/tail deletion, reordering, hardlinks, partial writes, and malformed
safety fields. Each process rejects a generation rollback or deletion after
observation.

Writes serialize on a fresh file descriptor for the record-root directory
inode, so replacing the visible `.outbound-records.lock` name cannot create
split-brain writers. The lock file remains a tamper sentinel. Root, ledger,
temporary file, and sentinel identity/type/owner/mode/link checks fail closed.

The HMAC ledger is not an external durable transparency log. A complete old
ledger plus matching HMAC can be replayed after every observing process exits,
or the whole root can be deleted before a new process starts. Preventing that
requires a governed durable monotonic anchor and trusted key provisioning.
Those are not implemented here.

## Blocking gates

The following remain mandatory blockers:

- `external_outbound_census_not_verified`
- `unclassified_executable_modes_not_resolved`
- `runtime_egress_trace_not_complete`
- `dynamic_import_trace_not_complete`
- `skill_trace_not_complete`
- `subprocess_descendant_trace_not_complete`
- `candidate_integration_not_verified`
- `deny_network_containment_not_verified`
- `credential_stripping_not_verified`
- `trusted_record_key_provisioning_not_verified`
- `durable_external_ledger_anchor_not_implemented`
- `record_root_filesystem_semantics_not_attested`
- `trusted_clock_not_integrated`

Secret/key-name, nested JSON, authorization token, credential URL, HTTP query,
and known identifier rejection are defense in depth, not a credential census.
Candidate startup must strip real credentials, disable all platform/cron/tool
dispatch, integrate this sink at every classified outbound construction point,
and pass independent deny-network plus runtime egress tracing. Until every
blocking gate is externally verified, this prototype is No-Go for candidate
execution, promotion, cutover, external delivery, and production.

## Verification

```sh
./tests/run.sh
/usr/local/bin/mypy census_binding.py record_only_outbound.py
PYTHONPYCACHEPREFIX=/tmp/hermes-record-only-pycache \
  /usr/bin/python3 -m py_compile census_binding.py record_only_outbound.py \
  tests/test_census_binding.py tests/test_record_only_outbound.py
shasum -a 256 -c REVIEW_SHA256SUMS
```

The adversarial suite covers superseded v1-v3 selection, the old v2 digest,
path traversal, symlink/hardlink inputs, path-replacement TOCTOU, duplicate or
mismatched fields, digest mismatch, provenance/count/mode anomalies, non-pending
rows, and re-sealed ledger authorization/binding changes. Passing it does not
establish census coverage or authorize candidate execution.
