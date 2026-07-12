"""Daily quota accounting for G1Q3 RCA auto-download grants.

The host decides per intake whether the VM pipeline may execute the PDCL
``mdi download`` (execution_policy.allow_download).  Grants are budgeted per
calendar day so a burst of issue triggers cannot saturate VM disk/bandwidth.

Quota source: ``HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA`` (default 0 = auto
download disabled; M1 keeps this at 0 and operators trigger the pipeline
manually, M2 raises it per the rollout ladder).

Accounting is a JSON file per day under the quota dir, guarded by flock so
concurrent intake threads cannot double-spend a grant.
"""

from __future__ import annotations

import fcntl
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

QUOTA_ENV = "HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA"


def _daily_quota_from_env() -> int:
    raw = os.getenv(QUOTA_ENV, "").strip()
    try:
        return max(0, int(raw)) if raw else 0
    except ValueError:
        return 0


def consume_g1q3_download_grant(
    *,
    quota_dir: str | Path,
    task_id: str,
    daily_quota: int | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    """Atomically consume one auto-download grant for today, if available.

    Returns ``{"granted", "used", "quota", "reason"}``.  Never raises: any
    accounting failure denies the grant (fail closed — a missed download is
    retryable, an unbudgeted download is not).
    """
    quota = _daily_quota_from_env() if daily_quota is None else max(0, int(daily_quota))
    if quota <= 0:
        return {"granted": False, "used": 0, "quota": quota, "reason": "auto_download_disabled"}

    day = (today or datetime.now(timezone.utc).date()).isoformat()
    try:
        out_dir = Path(quota_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        ledger_path = out_dir / f"g1q3_auto_download-{day}.json"
        with open(ledger_path, "a+", encoding="utf-8") as fh:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            fh.seek(0)
            raw = fh.read().strip()
            try:
                ledger = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                ledger = {}
            if not isinstance(ledger, dict) or ledger.get("date") != day:
                ledger = {"date": day, "used": 0, "grants": []}
            used = int(ledger.get("used") or 0)
            if used >= quota:
                return {"granted": False, "used": used, "quota": quota, "reason": "daily_quota_exhausted"}
            ledger["used"] = used + 1
            grants = ledger.get("grants") if isinstance(ledger.get("grants"), list) else []
            grants.append({"task_id": str(task_id or ""), "at": datetime.now(timezone.utc).isoformat()})
            ledger["grants"] = grants[-200:]
            fh.seek(0)
            fh.truncate()
            fh.write(json.dumps(ledger, ensure_ascii=False, indent=2) + "\n")
            return {"granted": True, "used": used + 1, "quota": quota, "reason": ""}
    except OSError as exc:
        return {"granted": False, "used": 0, "quota": quota, "reason": f"quota_ledger_error: {exc}"[:200]}
