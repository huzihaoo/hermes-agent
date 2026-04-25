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
