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

# This watchdog answers one question only: is a long-running resident stale?
# The full release gate also reports golden/evidence drift and is intentionally
# not used as a watcher freshness signal.
out="$(/usr/bin/python3 "$LIVE_EXEC" local.pnc.release-fingerprint-check --watchers-fresh "${vm_args[@]}" --json 2>&1)"
rc=$?
summary="$(printf '%s' "$out" | /usr/bin/python3 -c 'import sys,json
try:
    d=json.load(sys.stdin)
except Exception:
    print("|invalid_json")
    raise SystemExit(1)
stale=sorted(str(r.get("face")) for r in d.get("results", []) if r.get("actual") == "STALE")
errors=[]
for item in d.get("errors", []):
    text=str(item)
    errors.append(text.split(":", 1)[0])
print(",".join(stale) + "|" + ",".join(sorted(set(errors))))' 2>/dev/null)"
parse_rc=$?
stale="${summary%%|*}"
errors="${summary#*|}"
if { [ "$rc" -ne 0 ] || [ "$parse_rc" -ne 0 ]; } && [ -z "$stale" ]; then
  stale="release-fingerprint-check"
  [ "$rc" -eq 0 ] && rc=1
fi

if [ "$rc" -eq 0 ] && [ -z "$stale" ]; then
  echo "$(ts) OK rc=0 faces=[] errors=[$errors]" >> "$LOG"
  : > "$STATE" 2>/dev/null
  exit 0
fi

echo "$(ts) STALE rc=$rc faces=[$stale] errors=[$errors]" >> "$LOG"
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
