#!/usr/bin/env python3
"""CLI tool to inspect admission queue status.

Usage:
    python -m gateway.admission.cli status
    python -m gateway.admission.cli status --domain user
    python -m gateway.admission.cli status --domain-id alice
    python -m gateway.admission.cli clear [lane]
"""

import argparse
import json
import sys

from gateway.admission import AdmissionController
from gateway.admission.templates import PolicyTemplate, TemplateStore
from gateway.admission.types import ALL_DOMAINS, ALL_LANES


def cmd_status(args):
    """Show current queue status."""
    ctrl = AdmissionController()
    domain = args.domain if hasattr(args, "domain") else None
    domain_id = args.domain_id if hasattr(args, "domain_id") else None
    status = ctrl.get_status(domain=domain, domain_id=domain_id)

    if args.json:
        print(json.dumps(status, indent=2, ensure_ascii=False))
        return

    print("=== Admission Queue Status ===\n")

    domain_emoji = {"user": "👤", "group": "👥", "vm": "🖥️"}
    lane_emoji = {"fast": "⚡", "standard": "📝", "heavy": "🔨"}

    for d in ALL_DOMAINS:
        if d not in status:
            continue
        d_data = status[d]
        d_total = sum(
            lane_info["pending"]
            for did_data in d_data.values()
            for lane_info in did_data.values()
        )
        print(f"{domain_emoji[d]} {d.upper()} ({d_total} pending)")

        for did, did_data in sorted(d_data.items()):
            did_total = sum(v["pending"] for v in did_data.values())
            print(f"  📌 {did} ({did_total})")
            for lane in ALL_LANES:
                if lane not in did_data:
                    continue
                l_data = did_data[lane]
                print(f"    {lane_emoji[lane]} {lane}: {l_data['pending']}")
                for item in l_data["items"]:
                    print(f"      - {item['user_id']} ({item['user_role']}, pri={item['priority']})")
                    print(f"        {item['message_preview']}")
        print()

    metrics = status["metrics"]
    print("=== Metrics ===")
    print(f"Total admitted:  {metrics['total_admitted']}")
    print(f"Total rejected:  {metrics['total_rejected']}")
    print(f"Total completed: {metrics['total_completed']}")
    print(f"Total failed:    {metrics['total_failed']}")


def cmd_clear(args):
    """Clear queue (for testing/debugging)."""
    ctrl = AdmissionController()

    items = ctrl.queue.list_pending(lane=args.lane)
    for item in items:
        ctrl.queue.cancel(item.id)
    ctrl.queue.save()

    label = f"{args.lane} lane" if args.lane else "all lanes"
    print(f"Cleared {len(items)} items from {label}")


def cmd_template(args):
    """Manage policy templates."""
    from pathlib import Path as P

    store = TemplateStore(store_dir=P(args.store_dir) if args.store_dir else None)

    if args.action == "list":
        names = store.list_names()
        if not names:
            print("No templates found. Run 'template seed' to create built-ins.")
            return
        for n in names:
            t = store.get(n)
            desc = t.description if t else ""
            print(f"  {n:20s}  {desc}")
        return

    if args.action == "seed":
        store.seed_builtins()
        print(f"Seeded {len(store.list_names())} built-in templates.")
        return

    if args.action == "export":
        if not args.name or not args.path:
            print("Usage: template export --name NAME --path FILE", file=sys.stderr)
            sys.exit(1)
        store.export_template(args.name, P(args.path))
        print(f"Exported '{args.name}' → {args.path}")
        return

    if args.action == "import":
        if not args.path:
            print("Usage: template import --path FILE", file=sys.stderr)
            sys.exit(1)
        t = store.import_template(P(args.path))
        print(f"Imported '{t.name}' from {args.path}")
        return

    print(f"Unknown template action: {args.action}", file=sys.stderr)
    sys.exit(1)


def cmd_alerts(args, controller=None):
    """Show alert history."""
    ctrl = controller or AdmissionController()
    limit = getattr(args, "limit", 50)
    history = ctrl.get_alert_history(limit=limit)
    if not history:
        print("No alerts fired.")
        return
    for rec in history:
        import time as _t
        ts = _t.strftime("%Y-%m-%d %H:%M:%S", _t.localtime(rec.timestamp))
        print(f"  [{rec.level.value.upper()}] {ts}  {rec.message}")


def cmd_apply(args, controller=None):
    """Apply a policy template to the controller."""
    from pathlib import Path as P
    store = TemplateStore(store_dir=P(args.store_dir) if getattr(args, "store_dir", None) else None)
    tpl = store.get(args.name)
    if tpl is None:
        print(f"Template '{args.name}' not found. Run 'template list' to see available.")
        return
    ctrl = controller or AdmissionController()
    ctrl.apply_template(tpl)
    print(f"Applied template '{tpl.name}' — rate_limit={tpl.rate_limit_per_user}, "
          f"depth_warning={tpl.depth_warning}, depth_critical={tpl.depth_critical}")


def main():
    parser = argparse.ArgumentParser(description="Admission queue CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # status command
    sp = subparsers.add_parser("status", help="Show queue status")
    sp.add_argument("--json", action="store_true", help="Output as JSON")
    sp.add_argument("--domain", choices=list(ALL_DOMAINS), help="Filter by domain")
    sp.add_argument("--domain-id", dest="domain_id", help="Filter by domain_id")
    sp.set_defaults(func=cmd_status)

    # clear command
    cp = subparsers.add_parser("clear", help="Clear queue (testing only)")
    cp.add_argument("lane", nargs="?", choices=list(ALL_LANES),
                    help="Lane to clear (omit to clear all)")
    cp.set_defaults(func=cmd_clear)

    # template command
    tp = subparsers.add_parser("template", help="Manage policy templates")
    tp.add_argument("action", choices=["list", "seed", "export", "import"],
                    help="Template action")
    tp.add_argument("--name", help="Template name (for export)")
    tp.add_argument("--path", help="File path (for export/import)")
    tp.add_argument("--store-dir", dest="store_dir", help="Custom template store directory")
    tp.set_defaults(func=cmd_template)

    # alerts command
    ap = subparsers.add_parser("alerts", help="Show alert history")
    ap.add_argument("--limit", type=int, default=50, help="Max alert records to show")
    ap.set_defaults(func=cmd_alerts)

    # apply command
    pp = subparsers.add_parser("apply", help="Apply a policy template")
    pp.add_argument("name", help="Template name")
    pp.add_argument("--store-dir", dest="store_dir", help="Custom template store directory")
    pp.set_defaults(func=cmd_apply)

    args = parser.parse_args()

    try:
        args.func(args)
    except KeyboardInterrupt:
        print("\nInterrupted", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
