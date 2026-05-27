#!/usr/bin/env python3
"""Windowed Codex shared-state runner with retry and artifact reconciliation.

Runtime dist utility for /home/mini/.hermes/worker-state experiments.
It expects tasks to already exist in shared-state and have codex backend metadata.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, '/home/mini/.hermes/worker-state')
import shared_state_v2  # type: ignore

ROOT = Path('/home/mini/.hermes/shared-state')
WORKER = Path('/home/mini/.hermes/worker-state')
REPO_ROOT = '/home/mini/minieye_dnp_nop'


def now() -> str:
    return datetime.now().isoformat(timespec='seconds')



def parse_ts(value: str) -> float | None:
    try:
        return datetime.fromisoformat(value.strip()).timestamp()
    except Exception:
        return None


def qstate(root: Path, tid: str) -> str | None:
    for q in ['done', 'failed', 'claimed', 'pending']:
        if (root / 'dispatch' / q / f'{tid}.json').exists():
            return q
    return None


def read_json(path: Path) -> dict:
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def read_text(path: Path) -> str:
    try:
        return path.read_text(errors='replace')
    except Exception:
        return ''



def parse_waves(path: Path) -> list[dict]:
    waves: list[dict] = []
    for raw in read_text(path).splitlines():
        line = raw.strip()
        if not line:
            continue
        ts_text, _, rest = line.partition(' start count=')
        if not rest:
            waves.append({'raw': line, 'parse_error': 'missing_start_marker'})
            continue
        count_text, _, ids_text = rest.partition(' ids=')
        try:
            count = int(count_text)
        except ValueError:
            count = None
        ids: list[str] = []
        if ids_text.startswith('[') and ids_text.endswith(']'):
            for item in ids_text.strip('[]').split(','):
                item = item.strip().strip("'").strip('"')
                if item:
                    ids.append(item)
        waves.append({'timestamp': ts_text, 'count': count, 'ids': ids, 'raw': line})
    return waves


def collect_rows(root: Path, worker: Path, ids: list[str], expected_prefix: str | None) -> list[dict]:
    rows = []
    for idx, tid in enumerate(ids, 1):
        msg = expected_message(root, tid, expected_prefix)
        rows.append({'i': idx, 'task_id': tid, 'state': qstate(root, tid), 'ok': terminal_ok(root, worker, tid, msg)})
    return rows


def build_performance_summary(
    *,
    args: argparse.Namespace,
    out: Path,
    rows: list[dict],
    started_at: str | None,
    finished_at: str | None,
    retries: dict[str, int] | None = None,
) -> dict:
    waves = parse_waves(out / 'waves.log')
    start_epoch = parse_ts(started_at or '')
    finish_epoch = parse_ts(finished_at or '')
    wall_seconds = round(finish_epoch - start_epoch, 3) if start_epoch is not None and finish_epoch is not None else None
    counts_by_task: dict[str, int] = {}
    effective_max_wave_count = 0
    for wave in waves:
        count = wave.get('count')
        if isinstance(count, int):
            effective_max_wave_count = max(effective_max_wave_count, count)
        for tid in wave.get('ids') or []:
            counts_by_task[tid] = counts_by_task.get(tid, 0) + 1
    duplicate_starts = sorted(tid for tid, count in counts_by_task.items() if count > 1)
    retry_total = sum((retries or {}).values())
    ok_count = sum(1 for r in rows if r.get('ok'))
    return {
        'base': args.base,
        'count': args.count,
        'started_at': started_at,
        'finished_at': finished_at,
        'wall_seconds': wall_seconds,
        'tasks_per_minute': round(ok_count / (wall_seconds / 60), 3) if wall_seconds and wall_seconds > 0 else None,
        'configured_window': args.window,
        'retry_window': args.retry_window,
        'max_retries': args.max_retries,
        'ok_count': ok_count,
        'done': sum(r.get('state') == 'done' for r in rows),
        'failed': sum(r.get('state') == 'failed' for r in rows),
        'claimed': sum(r.get('state') == 'claimed' for r in rows),
        'pending': sum(r.get('state') == 'pending' for r in rows),
        'waves_count': len(waves),
        'effective_max_wave_count': effective_max_wave_count,
        'started_task_count': len(counts_by_task),
        'duplicate_start_task_ids': duplicate_starts,
        'retry_count_total': retry_total,
        'retry_counts': retries or {},
        'rows': rows,
        'telemetry_version': 1,
    }


def pid_alive(pid: object) -> bool:
    try:
        if not pid:
            return False
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def expected_message(root: Path, tid: str, default_prefix: str | None = None) -> str:
    meta = read_json(root / 'tasks' / tid / 'meta.json')
    msg = str(meta.get('expected_final_message') or '').strip()
    if msg:
        return msg
    return f'{default_prefix or "CODEX_OK"} {tid}'


def terminal_ok(root: Path, worker: Path, tid: str, msg: str) -> bool:
    result = read_text(root / 'tasks' / tid / 'result.md')
    last = read_text(worker / 'tasks' / tid / 'artifacts' / 'codex-last-message.txt')
    return ('- state: completed' in result) and (msg in last or msg in result)


def move_dispatch(root: Path, tid: str, src_q: str, dst_q: str, payload: dict) -> None:
    src = root / 'dispatch' / src_q / f'{tid}.json'
    dst = root / 'dispatch' / dst_q / f'{tid}.json'
    if dst.exists():
        dst.unlink()
    if src.exists():
        src.replace(dst)
    dst.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def update_db(root: Path, tid: str, state: str, summary: str, event_type: str, payload: dict) -> None:
    ts = now()
    con = sqlite3.connect(root / 'state.db')
    try:
        con.execute(
            'UPDATE tasks SET state=?, updated_at=?, heartbeat_at=?, lease_until=?, latest_summary=?, last_error=?, sync_version=COALESCE(sync_version,0)+1 WHERE task_id=?',
            (state, ts, ts if state == 'completed' else None, None, summary, None if state == 'completed' else summary, tid),
        )
        con.execute(
            'INSERT INTO task_events(task_id,event_type,payload_json,created_at) VALUES(?,?,?,?)',
            (tid, event_type, json.dumps(payload, ensure_ascii=False, sort_keys=True), ts),
        )
        con.commit()
    finally:
        con.close()


def reconcile_artifact_success(root: Path, worker: Path, tid: str, msg: str) -> bool:
    last = worker / 'tasks' / tid / 'artifacts' / 'codex-last-message.txt'
    runner = worker / 'tasks' / tid / 'artifacts' / 'runner.log'
    result_text = read_text(root / 'tasks' / tid / 'result.md')
    if not ((last.exists() and msg in read_text(last)) or msg in result_text):
        return False
    if not last.exists() or msg not in read_text(last):
        last.parent.mkdir(parents=True, exist_ok=True)
        last.write_text(msg + '\n', encoding='utf-8')
    tdir = root / 'tasks' / tid
    wdir = worker / 'tasks' / tid
    ts = now()
    summary = f'{tid} completed via artifact reconciler'
    result_payload = {
        'task_id': tid,
        'run_id': f'worker-{tid}',
        'repo_root': REPO_ROOT,
        'canonical_task_dir': str(tdir),
        'goal_path': str(tdir / 'goal.md'),
        'runner_log': str(runner),
        'artifact_root': str(wdir / 'artifacts'),
        'artifacts': [
            {'path': str(last), 'relative_path': 'codex-last-message.txt', 'size_bytes': last.stat().st_size},
            {'path': str(runner), 'relative_path': 'runner.log', 'size_bytes': runner.stat().st_size if runner.exists() else 0},
        ],
        'result_mode': 'structured-result-artifact-only',
        'report_contract': 'V4-V8',
        'manual_import_reason': 'last_message_artifact_present',
    }
    local_result = {'task_id': tid, 'state': 'completed', 'completed_at': ts, 'exit_code': 0, 'summary': summary, 'result': result_payload}
    (wdir / 'local-result.json').write_text(json.dumps(local_result, ensure_ascii=False, indent=2), encoding='utf-8')
    result_md = "# Result: {tid}\n\n- state: completed\n- summary: {summary}\n- exit_code: 0\n- completed_at: {ts}\n\n## Artifacts\n\n- codex-last-message.txt\n- runner.log\n\n## Result JSON\n\n```json\n{payload}\n```\n".format(tid=tid, summary=summary, ts=ts, payload=json.dumps(result_payload, ensure_ascii=False, indent=2))
    (tdir / 'result.md').write_text(result_md, encoding='utf-8')
    status_md = "# Status: {tid}\n\n- state: completed\n- summary: {summary}\n- updated_at: {ts}\n- heartbeat_at: {ts}\n- lease_until: \n- run_id: worker-{tid}\n- agent_host: mini-desktop\n- sync_version: 999\n- stale: false\n".format(tid=tid, summary=summary, ts=ts)
    (tdir / 'status.md').write_text(status_md, encoding='utf-8')
    q = qstate(root, tid)
    payload = read_json(root / 'dispatch' / (q or 'claimed') / f'{tid}.json')
    payload.update({'state': 'completed', 'updated_at': ts, 'heartbeat_at': ts, 'lease_until': None, 'summary': summary})
    if q != 'done':
        if q:
            move_dispatch(root, tid, q, 'done', payload)
        else:
            (root / 'dispatch' / 'done' / f'{tid}.json').write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    update_db(root, tid, 'completed', summary, 'artifact_reconciled', local_result)
    return True


def requeue_failed_timeout(root: Path, worker: Path, tid: str, retry_count: int) -> bool:
    if qstate(root, tid) != 'failed':
        return False
    lr = read_json(worker / 'tasks' / tid / 'local-result.json')
    text = json.dumps(lr, ensure_ascii=False)
    if 'codex_timeout' not in text and lr.get('exit_code') != 124:
        return False
    ts = now()
    src = root / 'dispatch' / 'failed' / f'{tid}.json'
    dst = root / 'dispatch' / 'pending' / f'{tid}.json'
    data = read_json(src)
    data.update({'state': 'pending', 'updated_at': ts, 'heartbeat_at': None, 'lease_until': None, 'summary': f'pending retry {retry_count} after codex_timeout', 'retry_count': retry_count, 'retry_reason': 'codex_timeout'})
    if dst.exists():
        dst.unlink()
    src.replace(dst)
    dst.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    for p in [worker / 'tasks' / tid / 'local-result.json', worker / 'tasks' / tid / 'artifacts' / 'codex-last-message.txt']:
        try:
            p.unlink()
        except FileNotFoundError:
            pass
    update_db(root, tid, 'pending', data['summary'], 'timeout_requeued', data)
    return True


def launch(root: Path, worker: Path, tid: str, out: Path) -> subprocess.Popen | None:
    if qstate(root, tid) == 'pending':
        claimed = shared_state_v2.claim_pending_batch(root=root, limit=1, lease_seconds=420, agent_host='codex-window-runner', task_id=tid)
        if not claimed:
            return None
    if qstate(root, tid) != 'claimed':
        return None
    cmd = [
        'python3', str(worker / 'vm_coding_worker_v2.py'),
        '--root', str(root), '--worker-root', str(worker), '--repo-root', REPO_ROOT,
        '--execute-claimed-task-id', tid,
        '--lease-seconds', '420', '--heartbeat-seconds', '10', '--timeout-seconds', '420', '--json', '--compact-status',
    ]
    f = open(out / f'{tid}.out', 'ab')
    return subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--base', required=True)
    ap.add_argument('--count', type=int, required=True)
    ap.add_argument('--window', type=int, default=24)
    ap.add_argument('--retry-window', type=int, default=4)
    ap.add_argument('--max-retries', type=int, default=1)
    ap.add_argument('--out', required=True)
    ap.add_argument('--expected-prefix', default=None)
    ap.add_argument('--telemetry-only', action='store_true', help='Only write summary/performance for existing task state; do not launch workers.')
    ap.add_argument('--started-at', default=None, help='Optional ISO timestamp for telemetry-only wall time.')
    ap.add_argument('--finished-at', default=None, help='Optional ISO timestamp for telemetry-only wall time.')
    args = ap.parse_args()
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    ids = [f'{args.base}-{i}' for i in range(1, args.count + 1)]
    retries = {tid: 0 for tid in ids}
    running: dict[str, subprocess.Popen] = {}
    started = time.time()
    started_at = args.started_at or now()
    if args.telemetry_only:
        rows = collect_rows(ROOT, WORKER, ids, args.expected_prefix)
        finished_at = args.finished_at or now()
        summary = {'base': args.base, 'count': args.count, 'window': args.window, 'retry_window': args.retry_window, 'max_retries': args.max_retries, 'ok_count': sum(r['ok'] for r in rows), 'done': sum(r['state'] == 'done' for r in rows), 'failed': sum(r['state'] == 'failed' for r in rows), 'claimed': sum(r['state'] == 'claimed' for r in rows), 'pending': sum(r['state'] == 'pending' for r in rows), 'failures': [r for r in rows if not r['ok']]}
        (out / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
        performance = build_performance_summary(args=args, out=out, rows=rows, started_at=started_at, finished_at=finished_at, retries=retries)
        (out / 'performance.json').write_text(json.dumps(performance, ensure_ascii=False, indent=2), encoding='utf-8')
        print(json.dumps(summary, ensure_ascii=False))
        return 0
    while True:
        # Reconcile artifact successes first.
        for tid in ids:
            if qstate(ROOT, tid) in {'claimed', 'failed'}:
                reconcile_artifact_success(ROOT, WORKER, tid, expected_message(ROOT, tid, args.expected_prefix))
        incomplete = [tid for tid in ids if qstate(ROOT, tid) in {'pending', 'claimed', 'failed'} and not terminal_ok(ROOT, WORKER, tid, expected_message(ROOT, tid, args.expected_prefix))]
        # Retry timeout failures up to max.
        for tid in list(incomplete):
            if qstate(ROOT, tid) == 'failed' and retries[tid] < args.max_retries:
                retries[tid] += 1
                requeue_failed_timeout(ROOT, WORKER, tid, retries[tid])
        for tid, proc in list(running.items()):
            if proc.poll() is not None:
                running.pop(tid, None)
        externally_running = []
        stale_claimed = []
        for tid in ids:
            if qstate(ROOT, tid) == 'claimed':
                pid = read_json(WORKER / 'tasks' / tid / 'local-status.json').get('pid')
                if tid in running or pid_alive(pid):
                    externally_running.append(tid)
                else:
                    stale_claimed.append(tid)
        pending = [tid for tid in ids if qstate(ROOT, tid) == 'pending']
        if not pending and not stale_claimed and not running and not externally_running:
            break
        cap = max(0, args.window - len(running) - len(externally_running))
        batch = stale_claimed[:cap]
        cap -= len(batch)
        batch += pending[:cap]
        if batch:
            with (out / 'waves.log').open('a') as log:
                log.write(f'{now()} start count={len(batch)} ids={batch}\n')
            for tid in batch:
                proc = launch(ROOT, WORKER, tid, out)
                if proc:
                    running[tid] = proc
        if time.time() - started > 3600:
            break
        time.sleep(5)
    rows = collect_rows(ROOT, WORKER, ids, args.expected_prefix)
    finished_at = now()
    summary = {'base': args.base, 'count': args.count, 'window': args.window, 'retry_window': args.retry_window, 'max_retries': args.max_retries, 'ok_count': sum(r['ok'] for r in rows), 'done': sum(r['state'] == 'done' for r in rows), 'failed': sum(r['state'] == 'failed' for r in rows), 'claimed': sum(r['state'] == 'claimed' for r in rows), 'pending': sum(r['state'] == 'pending' for r in rows), 'failures': [r for r in rows if not r['ok']]}
    (out / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    performance = build_performance_summary(args=args, out=out, rows=rows, started_at=started_at, finished_at=finished_at, retries=retries)
    (out / 'performance.json').write_text(json.dumps(performance, ensure_ascii=False, indent=2), encoding='utf-8')
    shared_state_v2.rebuild_index(ROOT)
    print(json.dumps(summary, ensure_ascii=False))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
