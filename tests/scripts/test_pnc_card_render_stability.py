"""Card-render stability regression for the 2026-06-26 silent two-writer flap.

Root cause: `_append_milestone` advanced an existing milestone's ts whenever it
was re-appended with a later ts. The status-confirmation milestones are stamped
with `notice.generated_at or meta.updated_at`, and meta.updated_at is bumped on
every passive re-sync (vm_task_sync upserts / log appends). So every full scan
re-stamped the milestones, changed the rendered card hash, and re-PATCHed the
card — a silent flap (no new messages, but wasteful and the same root that the
fault_class regression amplified). The fix: a milestone is a point-in-time event;
once recorded its ts is frozen (only backfilled when missing).
"""
from scripts.pnc_completion_notice_relay import _append_milestone


def test_milestone_ts_frozen_on_resync_no_drift():
    ms = []
    _append_milestone(ms, "报告状态确认：html_delivery_ready", "2026-06-24T14:00:00+08:00")
    assert len(ms) == 1
    first_ts = ms[0]["ts"]
    # later full scan re-syncs with a bumped meta.updated_at -> must NOT drift
    _append_milestone(ms, "报告状态确认：html_delivery_ready", "2026-06-26T15:40:30+08:00")
    _append_milestone(ms, "报告状态确认：html_delivery_ready", "2026-06-26T16:10:00+08:00")
    assert len(ms) == 1, "must dedupe by label, not append duplicates"
    assert ms[0]["ts"] == first_ts, "milestone ts drifted forward -> card render flap (flood risk)"


def test_milestone_dedupes_by_semantic_label():
    ms = []
    _append_milestone(ms, "归因状态确认：hypothesis_ready", "2026-06-24T14:00:00+08:00")
    _append_milestone(ms, "归因状态确认：hypothesis_ready", "2026-06-26T15:00:00+08:00")
    assert len(ms) == 1


def test_milestone_backfills_missing_ts_once():
    # If a milestone was recorded without a parseable ts, the first append that
    # carries one backfills it (one-time); subsequent appends do not drift it.
    ms = [{"ts": "", "label": "报告状态确认：html_delivery_ready"}]
    _append_milestone(ms, "报告状态确认：html_delivery_ready", "2026-06-24T14:00:00+08:00")
    assert len(ms) == 1
    backfilled = ms[0]["ts"]
    assert backfilled  # got a ts
    _append_milestone(ms, "报告状态确认：html_delivery_ready", "2026-06-26T15:40:30+08:00")
    assert ms[0]["ts"] == backfilled, "ts drifted after backfill"


def test_distinct_milestones_each_keep_own_ts():
    ms = []
    _append_milestone(ms, "已接单，开始读取飞书问题", "2026-06-24T13:00:00+08:00")
    _append_milestone(ms, "报告状态确认：html_delivery_ready", "2026-06-24T14:00:00+08:00")
    assert len(ms) == 2
    # re-sync both with later ts -> neither drifts
    _append_milestone(ms, "已接单，开始读取飞书问题", "2026-06-26T15:00:00+08:00")
    _append_milestone(ms, "报告状态确认：html_delivery_ready", "2026-06-26T15:40:00+08:00")
    assert len(ms) == 2
    labels_ts = {m["label"]: m["ts"] for m in ms}
    # both kept their original (earlier) timestamps
    assert "13:00" in labels_ts.get("已接单，开始读取飞书问题", "") or "2026-06-24" in labels_ts.get("已接单，开始读取飞书问题", "")
