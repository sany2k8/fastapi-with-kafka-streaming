"""Tiny stdin JSON formatter for the demo scripts (stdlib only, py3.8+)."""

import json
import sys


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "table"
    data = json.load(sys.stdin)

    if mode == "accepted":
        print("  {0}  {1}".format(data["payment_id"], data["status"]))
        return

    if mode == "kafka":
        for topic, info in data["topics"].items():
            print("  {0}  ({1} partitions, {2} records)".format(
                topic, info["partition_count"], info["total_records"]
            ))
            for part in info["partitions"]:
                print("      partition {0}  end_offset={1}".format(
                    part["partition"], part["end_offset"]
                ))
        print()
        for group, info in data["consumer_groups"].items():
            print("  group {0}  [{1}]  members={2}  total_lag={3}".format(
                group, info["state"], info.get("member_count", 0), info["total_lag"]
            ))
            for member in info["members"]:
                print("      owns {0}".format(member["assigned_partitions"] or "nothing (idle)"))
            for row in info["offsets"]:
                print("      {0}-{1}  committed={2}  end={3}  lag={4}".format(
                    row["topic"], row["partition"], row["committed_offset"],
                    row["end_offset"], row["lag"],
                ))
        return

    header = "  {0:<24} {1:<8} {2:<11} {3:>5}  {4}".format(
        "PAYMENT", "COUNTRY", "STATUS", "SCORE", "LEVEL"
    )
    print(header)
    print("  " + "-" * (len(header) + 6))
    for row in reversed(data):
        print(
            "  {0:<24} {1:<8} {2:<11} {3:>5}  {4}".format(
                row["payment_id"],
                row["country"],
                row["status"],
                row["risk_score"] if row["risk_score"] is not None else "-",
                row["risk_level"] or "",
            )
        )


if __name__ == "__main__":
    main()
