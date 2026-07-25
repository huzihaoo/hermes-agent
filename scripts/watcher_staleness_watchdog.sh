#!/bin/zsh
set -u

LOG="${HOME}/.hermes/logs/watcher-staleness-watchdog.log"
STATE="${HOME}/.hermes/logs/.watcher-staleness-alert-state"
GOVDIR="${HOME}/.hermes/runtime/governance-tools"
LIVE_EXEC="${GOVDIR}/pnc_live_exec.py"
mkdir -p "${HOME}/.hermes/logs"
ts() { date "+%Y-%m-%d %H:%M:%S"; }

vm_args=()
[ "${HERMES_WATCHER_STALENESS_VM:-0}" = "1" ] && vm_args=(--vm)

gate_out="$(/usr/bin/python3 "$LIVE_EXEC" local.pnc.release-freshness-gate --json 2>&1)"
gate_rc=$?
out=""
rc=0
stale=""
if [ "$gate_rc" -ne 0 ]; then
  rc="$gate_rc"
  stale="release-freshness-gate"
else
  out="$(/usr/bin/python3 "$LIVE_EXEC" local.pnc.release-fingerprint-check --watchers-fresh "${vm_args[@]}" --json 2>&1)"
  rc=$?
  stale="$(printf '%s' "$out" | /usr/bin/python3 -c 'import sys,json
try: d=json.load(sys.stdin)
except Exception: print(""); raise SystemExit
print(",".join(sorted(str(r.get("face")) for r in d.get("results",[]) if r.get("actual")=="STALE")))' 2>/dev/null)"
  if [ "$rc" -ne 0 ] && [ -z "$stale" ]; then
    stale="release-fingerprint-check"
  fi
fi

if [ "$rc" -eq 0 ] && [ -z "$stale" ]; then
  echo "$(ts) OK no stale long-running watchers" >> "$LOG"
  : > "$STATE" 2>/dev/null
  exit 0
fi

echo "$(ts) STALE rc=$rc faces=[$stale] gate=$gate_out fingerprint=$out" >> "$LOG"
chat="${HERMES_WATCHER_STALENESS_FEISHU_CHAT:-}"
[ -z "$chat" ] && exit 0

realert_h="${HERMES_WATCHER_STALENESS_REALERT_HOURS:-6}"
now=$(date +%s)
last_set=""
last_ts=0
if [ -f "$STATE" ]; then
  last_ts="$(sed -n '1p' "$STATE" 2>/dev/null)"
  last_set="$(sed -n '2p' "$STATE" 2>/dev/null)"
  [ -z "$last_ts" ] && last_ts=0
fi
window=$(( realert_h * 3600 ))
if [ "$stale" = "$last_set" ] && [ $(( now - last_ts )) -lt $window ]; then
  echo "$(ts) alert-suppressed (same stale-set, within ${realert_h}h re-alert window)" >> "$LOG"
  exit 0
fi

text="Hermes long-running watcher runtime is stale: ${stale}. Restart the named launchd label and inspect watcher-staleness-watchdog.log."
resp="$(/usr/bin/python3 "$LIVE_EXEC" local.pnc.feishu-ops-alert "$chat" "$text" 2>&1)"
arc=$?
echo "$(ts) alert-sent rc=$arc resp=$resp" >> "$LOG"
if [ "$arc" -eq 0 ]; then
  printf '%s\n%s\n' "$now" "$stale" > "$STATE"
fi
exit 0
