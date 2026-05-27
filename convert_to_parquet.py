"""
Convert OKX BTC-USD-SWAP L2 orderbook .data files to parquet format.

Each row in the output parquet represents one orderbook snapshot/update moment,
containing the top 10 levels of bids and asks (price, qty, ordcnt each).

Usage:
    python convert_to_parquet.py [input_files...] [-o output_dir]
"""

import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

BATCH_SIZE = 50000
TOP_N = 10


def parse_line(line):
    """Parse a JSON line into (action, ts, asks_list, bids_list)."""
    data = json.loads(line)
    action = data["action"]
    ts = int(data["ts"])
    asks = data.get("asks", [])
    bids = data.get("bids", [])
    return action, ts, asks, bids


def apply_update(orderbook, levels):
    """Apply update levels to an orderbook dict (price -> (qty, ordcnt))."""
    for level in levels:
        price = float(level[0])
        qty = float(level[1])
        ordcnt = int(level[2])
        if qty > 0:
            orderbook[price] = (qty, ordcnt)
        else:
            orderbook.pop(price, None)


def extract_top_n(orderbook, n, reverse=False):
    """Extract top n levels from orderbook.

    Returns lists of (price, qty, ordcnt), padded to length n with None.
    For asks: sorted ascending (lowest price first).
    For bids: sorted descending (highest price first).
    """
    sorted_prices = sorted(orderbook.keys(), reverse=reverse)
    result = []
    for price in sorted_prices[:n]:
        qty, ordcnt = orderbook[price]
        result.append((price, qty, ordcnt))
    while len(result) < n:
        result.append((None, None, None))
    return result


def build_row(ts, asks_orderbook, bids_orderbook):
    """Build a flat dict of 60 columns for one orderbook snapshot."""
    asks = extract_top_n(asks_orderbook, TOP_N, reverse=False)
    bids = extract_top_n(bids_orderbook, TOP_N, reverse=True)

    row = {"ts": ts}
    for i, (price, qty, ordcnt) in enumerate(asks, 1):
        row[f"a{i}_price"] = price
        row[f"a{i}_qty"] = qty
        row[f"a{i}_ordcnt"] = ordcnt
    for i, (price, qty, ordcnt) in enumerate(bids, 1):
        row[f"b{i}_price"] = price
        row[f"b{i}_qty"] = qty
        row[f"b{i}_ordcnt"] = ordcnt
    return row


def get_schema():
    """Define the parquet schema."""
    fields = [pa.field("ts", pa.int64())]
    for prefix in ("a", "b"):
        for i in range(1, TOP_N + 1):
            fields.append(pa.field(f"{prefix}{i}_price", pa.float64()))
            fields.append(pa.field(f"{prefix}{i}_qty", pa.float64()))
            fields.append(pa.field(f"{prefix}{i}_ordcnt", pa.float64()))
    return pa.schema(fields)


def flush_batch(writer, records, schema, output_path):
    """Convert a list of row dicts to an Arrow table and write it."""
    if not records:
        return writer
    arrays = []
    for field in schema:
        col_data = [r[field.name] for r in records]
        arrays.append(pa.array(col_data, type=field.type))
    table = pa.Table.from_arrays(arrays, schema=schema)
    if writer is None:
        writer = pq.ParquetWriter(output_path, schema, compression="snappy")
    writer.write_table(table)
    return writer


def convert_file(input_path, output_path):
    """Process one .data file and write to parquet.

    Reads the file line-by-line, maintains the orderbook state,
    and writes output in batches to limit memory usage.
    """
    print(f"Converting: {input_path}")
    print(f"Output:     {output_path}")

    schema = get_schema()
    asks_orderbook = {}
    bids_orderbook = {}
    writer = None
    batch = []
    total_rows = 0
    snapshot_count = 0
    update_count = 0
    line_count = 0

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(input_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            line_count += 1
            action, ts, asks_levels, bids_levels = parse_line(line)

            if action == "snapshot":
                asks_orderbook = {}
                bids_orderbook = {}
                for level in asks_levels:
                    price = float(level[0])
                    qty = float(level[1])
                    ordcnt = int(level[2])
                    if qty > 0:
                        asks_orderbook[price] = (qty, ordcnt)
                for level in bids_levels:
                    price = float(level[0])
                    qty = float(level[1])
                    ordcnt = int(level[2])
                    if qty > 0:
                        bids_orderbook[price] = (qty, ordcnt)
                snapshot_count += 1
            else:
                apply_update(asks_orderbook, asks_levels)
                apply_update(bids_orderbook, bids_levels)
                update_count += 1

            row = build_row(ts, asks_orderbook, bids_orderbook)
            batch.append(row)
            total_rows += 1

            if len(batch) >= BATCH_SIZE:
                writer = flush_batch(writer, batch, schema, output_path)
                batch = []
                print(f"  Processed {total_rows:,} rows...", end="\r")

    # Flush remaining records
    if batch:
        writer = flush_batch(writer, batch, schema, output_path)

    if writer is not None:
        writer.close()

    print(f"\nDone: {total_rows:,} rows "
          f"({snapshot_count} snapshots, {update_count} updates)")
    return total_rows


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Convert OKX L2 orderbook .data files to parquet format."
    )
    parser.add_argument(
        "inputs",
        nargs="*",
        default=[
            "BTC-USD-SWAP-L2orderbook-400lv-2026-05-23.data",
            "BTC-USD-SWAP-L2orderbook-400lv-2026-05-24.data",
        ],
        help="Input .data files (default: both day files)",
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="parquet_output",
        help="Output directory (default: parquet_output)",
    )
    args = parser.parse_args()

    for input_file in args.inputs:
        stem = Path(input_file).stem
        output_file = os.path.join(args.output_dir, f"{stem}.parquet")
        convert_file(input_file, output_file)


if __name__ == "__main__":
    main()
