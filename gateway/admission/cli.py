#!/usr/bin/env python3
"""CLI tool to inspect admission queue status.

Usage:
    python -m gateway.admission.cli status
    python -m gateway.admission.cli clear [lane]
"""

import argparse
import json
import sys
from pathlib import Path

from gateway.admission import AdmissionController


def cmd_status(args):
    """Show current queue status."""
    ctrl = AdmissionController()
    status = ctrl.get_status()
    
    if args.json:
        print(json.dumps(status, indent=2, ensure_ascii=False))
        return
    
    # Human-readable output
    print("=== Admission Queue Status ===\n")
    
    for lane in ["fast", "standard", "heavy"]:
        lane_data = status[lane]
        print(f"[{lane.upper()}] {lane_data['pending']} pending")
        
        if lane_data["items"]:
            for item in lane_data["items"]:
                print(f"  - {item['user_id']} ({item['user_role']}, pri={item['priority']})")
                print(f"    {item['message_preview']}")
                print(f"    created: {item['created_at']}")
        else:
            print("  (empty)")
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
    
    if args.lane:
        # Clear specific lane
        items = ctrl.queue.list_pending(args.lane)
        for item in items:
            ctrl.queue.cancel(item.id)
        ctrl.queue.save()
        print(f"Cleared {len(items)} items from {args.lane} lane")
    else:
        # Clear all lanes
        total = 0
        for lane in ["fast", "standard", "heavy"]:
            items = ctrl.queue.list_pending(lane)
            for item in items:
                ctrl.queue.cancel(item.id)
            total += len(items)
        ctrl.queue.save()
        print(f"Cleared {total} items from all lanes")


def main():
    parser = argparse.ArgumentParser(description="Admission queue CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # status command
    status_parser = subparsers.add_parser("status", help="Show queue status")
    status_parser.add_argument("--json", action="store_true", help="Output as JSON")
    status_parser.set_defaults(func=cmd_status)
    
    # clear command
    clear_parser = subparsers.add_parser("clear", help="Clear queue (testing only)")
    clear_parser.add_argument("lane", nargs="?", choices=["fast", "standard", "heavy"],
                             help="Lane to clear (omit to clear all)")
    clear_parser.set_defaults(func=cmd_clear)
    
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
