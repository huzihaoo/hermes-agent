# RCA Release Preflight

## Status: COMPLETE (candidate ready for review)

This is an uncommitted candidate with no production effect.  Its collector,
inventory, structured report, R4 coverage, and prepare-before-write integration
meet the corrected v4 acceptance contract.  The 2026-08-20 correction defines
read-only as no production semantic-state mutation, not zero file writes.  It
explicitly permits bounded temporary DB/WAL snapshots outside the production
directory and SQLite runtime sidecars while prohibiting business-table writes,
configuration or manifest/ledger/receipt changes, service restarts, and Feishu
writes.

The candidate uses the established WAL-aware
`RcaControlStore(read_only=True)` snapshot path, closes and cleans every
temporary snapshot, and does not mutate the source DB/WAL/SHM identities in the
real-store regression.  It deliberately does not use `immutable=1`, which can
omit active WAL state while the dispatcher is writing.  The corrected contract
therefore removes S6 without requiring a new storage mechanism.

The revised v4 card places A2 first at P0 and declares no A1 dependency.  The
preflight is a cross-line release-chain control for RCA and Feishu and is ready
for normal review/commit handling independently of A1.

`preflight-gate-inventory.csv` is generated from the Python AST of
`scripts/pnc_rca_minimal_release.py`, not from a text or `grep` count.  The
inventory records the current candidate source at commit `36e850e659d11fec87806f6bb362b22ad330e85b`
and must be regenerated when the driver moves.  The source hash observed before
this A2 change was:

```
c8c68be8fb57779362084a19479c0bc69d19a8acfd39a0d7a729b69126b3bafc
```

The historical task card says 126 error codes and 236 throw points.  A current
AST observation of the unchanged pre-A2 driver found 223 `ReleaseError` calls
plus 15 bare rethrows, or 238 physical throw points.  Of those calls, 203 use
literal strings and yield 128 unique literal codes; the other 20 derive their
code dynamically.  This is why a rough text count is invalid.

The task card's 126-code semantic baseline excludes the two nested emergency
fallback wrappers `terminal_receipt_write_failed` and
`terminal_receipt_and_marker_write_failed`.  Removing those two wrapper calls
from the physical AST also yields the authoritative 221 calls plus 15 bare
rethrows, or 236 semantic throw points.  Removing their two codes from the 128
live literal codes reproduces 126.  The inventory retains both as execution-only
rows so an operator can still trace every literal throw path.  The stabilized
A2 working tree observed at source SHA-256
`a435a5fa40120def736f209ac3c18d99b03db77d7d8dfcb1fad2a2af9f55a5e1`
has 129 unique literal codes.  The CSV contains all 129 plus fourteen dynamic
or supplemental collector gates: six semantic collector aliases and eight
absolute-path codes supplied dynamically to `_absolute`.  Its 143 unique rows,
including 43 precheckable rows, are sorted by gate code.  The stabilized test
source SHA-256 is
`576c14e570d9379dfe5f69101775968054954026be85f5e66b7e1c92288cb015`;
the regenerated inventory SHA-256 is
`53da47504800fb5fcb136c2b048befe8c14db09fa56eef9d20749f907b1b98ac`.
`precheckable=true` is limited to gates reached directly by the collector or
through the grouped probe named in `check_method`; plan/apply-only gates remain
false.

Dynamic transport variants such as a helper's `_readback_failed`,
`_unavailable`, `_identity_invalid`, or `_changed` code are reported in the
structured row's `failure_code`.  They remain grouped under the semantic
collector gate named by `gate` and `check_method`; they are not counted as
independent collector checks.  This distinction keeps the literal AST count,
registered collector-gate count, and runtime check count reproducible.

The R4 list is occurrence-based, not a set of ten unique strings: the source
evidence contains 11 attempts and 9 unique codes, with repeated snapshot and
identity gates.  The CSV keeps one row per gate code and the regression test
keeps every historical occurrence, including superseded
`prepare_control_schema_already_v15` and the external materializer gate.

## Read-only contract

The standalone `preflight` command evaluates every registered check and emits
`schema_version`, `checked`, `passed`, `deferred`, `deferred_count`, `total`,
`failed`, and `checks`.  `checked` counts only precheckable rows, `passed`
counts only successful precheckable rows, both deferred fields count
non-precheckable rows, and `total = checked + deferred`.  Each check has a
`passed`, `failed`, or `deferred` status plus its gate, actual value, expected
value, and repair hint.  Probe exceptions become a failed row; they do not stop
later checks.  The command never calls the fetching GitLab resolver, never
creates refs or temporary repositories, and never mutates release outputs,
source assets, the production control DB or its WAL/SHM sidecars, service
state, or Feishu.  It does not restart a service.

GitLab identity uses `git ls-remote --exit-code` only.  A ref-only probe cannot
prove a tree object without downloading objects, so tree comparison is an
explicit deferred/non-precheckable row; governed readback remains responsible
for the object-bearing tree check.

The control store is opened read-only for each independent probe.  Its bounded
temporary snapshot prevents SQLite from creating source sidecars; the snapshot
lease is closed and cleaned in a `finally` block before the next probe.  These
temporary DB/WAL snapshot files are explicitly allowed by the corrected
contract because they are outside the production directory and do not change
production semantic state.  A manual WAL-to-memory replay was evaluated and
rejected: SQLite WAL bytes have no intrinsic database UUID, valid
salts/checksums can be replayed against an unrelated same-schema database, and
salts legitimately rotate after checkpoint.  Accepting that adapter would
therefore create a fail-open cross-generation read path.  VM reads use
`ssh-mini-agent run_py_json` directly, without the mutating `doctor` pre-probe.
That wrapper's bounded temporary script directory is also cleaned when the call
ends.  No probe writes a business table, configuration, manifest, ledger,
receipt, service state, or Feishu state.

The remote-reader dependency gate uses the mandatory default read-only VM
probe.  It requires the candidate runtime root to be a stable, clean checkout
at the pipeline commit resolved by the read-only remote identity probe.  It
reads the contract blob from that commit and requires the checked-out
`api/g1q3_rca/vendor/remote_reader_runtime_contract.json` bytes to match.  It
then validates the exact `mcap`, `protobuf`, `pdcl-dss`, and `typer` dependency
set and independently fingerprints every installed VM distribution without
short-circuiting.  Each dependency retains its own version, path, RECORD, and
critical-file mismatch details.  It never imports the dependencies or executes
candidate-controlled code.  Bootstrap execution is explicitly deferred to the
execution-only `bootstrap_install-offline_failed` gate.  A stale or dirty
checkout, missing contract, schema mismatch, or byte/version mismatch is a
structured failure; the probe never installs.  The clean committed-contract
binding does not upgrade the separately deferred report-manifest tree-identity
gate.  Canary state is
checked by standalone preflight and intentionally deferred for `prepare`,
because prepare must not create a future activation artifact.

`prepare` invokes the same collector before reserving any output.  If it is
blocked, `PreflightError.result` and the CLI response retain the complete
aggregate failure list.

The v4 goal prose also names a `status` subcommand, but the observed baseline
parser exposed only `prepare`, `plan`, `apply`, and `verify`.  A2 follows the
live parser and adds only `preflight`; it does not introduce an unrelated
`status` command.
