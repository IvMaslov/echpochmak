"""PyIceberg writer for the HA tests.

Appends one row per step, (writer, seq), to a table through the catalog's
load balancer, and prints one JSON line per row it knows was committed.

Failures are classified like a careful production writer would:
- CommitFailedException (409): the commit certainly did not happen; retry.
- Anything else (5xx such as CommitStateUnknown, connection errors): the
  outcome is unknown. Before retrying, look for the row: if it is there,
  the commit landed; retrying blindly would duplicate it.

Usage:
    python writer.py --table db.ha --writer py --count 10
    python writer.py --table db.ha --writer py --duration 20 --start 100
"""

import argparse
import json
import os
import sys
import time

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, EqualTo

SCHEMA = pa.schema([pa.field("writer", pa.string()), pa.field("seq", pa.int64())])


def catalog():
    return load_catalog(
        "ha",
        **{
            "type": "rest",
            "uri": "http://catalog:8181",
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
            "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
            "s3.region": os.environ.get("S3_REGION", "us-east-1"),
        },
    )


def emit(**fields):
    print(json.dumps(fields), flush=True)


def landed(cat, table, writer, seq):
    """Report whether the row is in the table, retrying through outages."""
    while True:
        try:
            tbl = cat.load_table(table)
            filt = And(EqualTo("writer", writer), EqualTo("seq", seq))
            return tbl.scan(row_filter=filt).to_arrow().num_rows > 0
        except Exception as err:  # noqa: BLE001 - any failure: try again.
            emit(event="verify_error", seq=seq, error=repr(err)[:200])
            time.sleep(0.5)


def append(cat, table, writer, seq, deadline):
    """Commit the row exactly once; return the number of attempts, or None
    if the deadline passed first."""
    rows = pa.Table.from_pylist([{"writer": writer, "seq": seq}], schema=SCHEMA)
    attempts = 0

    while time.monotonic() < deadline:
        attempts += 1
        try:
            cat.load_table(table).append(rows)
            return attempts
        except CommitFailedException as err:
            emit(event="conflict", seq=seq, error=repr(err)[:200])
        except Exception as err:  # noqa: BLE001 - unknown outcome.
            emit(event="unknown", seq=seq, error=repr(err)[:200])
            if landed(cat, table, writer, seq):
                return attempts
        time.sleep(0.2)

    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--table", required=True)
    p.add_argument("--writer", required=True)
    p.add_argument("--count", type=int, default=0)
    p.add_argument("--duration", type=float, default=0)
    p.add_argument("--start", type=int, default=0)
    args = p.parse_args()

    cat = catalog()
    stop = time.monotonic() + args.duration if args.duration else None
    seq = args.start

    while True:
        if args.count and seq >= args.start + args.count:
            break
        if stop and time.monotonic() >= stop:
            break

        # A single row may need a long time while the cluster recovers.
        attempts = append(cat, args.table, args.writer, seq, time.monotonic() + 120)
        if attempts is None:
            emit(event="gave_up", seq=seq)
            sys.exit(1)

        emit(event="acked", seq=seq, attempts=attempts)
        seq += 1


if __name__ == "__main__":
    main()
