"""Tests for convert_to_parquet.py using synthetic orderbook data."""

import json
import os
import tempfile

import pyarrow.parquet as pq
import pytest

from convert_to_parquet import (
    apply_update,
    build_row,
    convert_file,
    extract_top_n,
    get_schema,
    parse_line,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_snapshot_line(asks, bids, ts="1000000"):
    """Build a JSON line with action=snapshot."""
    return json.dumps({
        "instId": "BTC-USD-SWAP",
        "action": "snapshot",
        "ts": ts,
        "asks": asks,
        "bids": bids,
    })


def make_update_line(asks, bids, ts="1000010"):
    """Build a JSON line with action=update."""
    return json.dumps({
        "instId": "BTC-USD-SWAP",
        "action": "update",
        "ts": ts,
        "asks": asks,
        "bids": bids,
    })


def write_tmp(lines):
    """Write lines to a temp .data file, return path."""
    fd, path = tempfile.mkstemp(suffix=".data")
    with os.fdopen(fd, "w") as f:
        for line in lines:
            f.write(line + "\n")
    return path


# ---------------------------------------------------------------------------
# Unit tests: parse_line
# ---------------------------------------------------------------------------

class TestParseLine:
    def test_snapshot(self):
        line = make_snapshot_line(
            [["100", "1.5", "2"], ["101", "3.0", "1"]],
            [["99", "2.0", "3"]],
            ts="5000",
        )
        action, ts, asks, bids = parse_line(line)
        assert action == "snapshot"
        assert ts == 5000
        assert len(asks) == 2
        assert len(bids) == 1
        assert asks[0] == ["100", "1.5", "2"]

    def test_update(self):
        line = make_update_line([["100", "0", "0"]], [], ts="5010")
        action, ts, asks, bids = parse_line(line)
        assert action == "update"
        assert ts == 5010
        assert len(asks) == 1
        assert len(bids) == 0


# ---------------------------------------------------------------------------
# Unit tests: apply_update
# ---------------------------------------------------------------------------

class TestApplyUpdate:
    def test_insert_new_price(self):
        ob = {}
        apply_update(ob, [["100", "5.0", "2"]])
        assert 100.0 in ob
        assert ob[100.0] == (5.0, 2)

    def test_update_existing_price(self):
        ob = {100.0: (5.0, 2)}
        apply_update(ob, [["100", "10.0", "3"]])
        assert ob[100.0] == (10.0, 3)

    def test_delete_existing_price(self):
        ob = {100.0: (5.0, 2), 101.0: (3.0, 1)}
        apply_update(ob, [["100", "0", "0"]])
        assert 100.0 not in ob
        assert 101.0 in ob

    def test_delete_nonexistent_price_noop(self):
        ob = {100.0: (5.0, 2)}
        apply_update(ob, [["200", "0", "0"]])
        assert len(ob) == 1  # unchanged

    def test_insert_with_zero_qty_noop(self):
        ob = {}
        apply_update(ob, [["100", "0", "0"]])
        assert len(ob) == 0

    def test_multiple_updates(self):
        ob = {100.0: (5.0, 2)}
        apply_update(ob, [
            ["100", "10.0", "3"],  # update
            ["101", "2.0", "1"],   # insert
            ["100", "0", "0"],     # delete
        ])
        assert 100.0 not in ob
        assert ob[101.0] == (2.0, 1)


# ---------------------------------------------------------------------------
# Unit tests: extract_top_n
# ---------------------------------------------------------------------------

class TestExtractTopN:
    def test_asks_ascending(self):
        """Asks should be sorted lowest price first."""
        ob = {100.0: (1.0, 1), 98.0: (2.0, 1), 102.0: (3.0, 1)}
        result = extract_top_n(ob, 10, reverse=False)
        prices = [r[0] for r in result[:3]]
        assert prices == [98.0, 100.0, 102.0]

    def test_bids_descending(self):
        """Bids should be sorted highest price first."""
        ob = {100.0: (1.0, 1), 98.0: (2.0, 1), 102.0: (3.0, 1)}
        result = extract_top_n(ob, 10, reverse=True)
        prices = [r[0] for r in result[:3]]
        assert prices == [102.0, 100.0, 98.0]

    def test_padding(self):
        """If fewer than N levels, remaining slots are None."""
        ob = {100.0: (1.0, 1)}
        result = extract_top_n(ob, 3, reverse=False)
        assert len(result) == 3
        assert result[0] == (100.0, 1.0, 1)
        assert result[1] == (None, None, None)
        assert result[2] == (None, None, None)

    def test_truncation(self):
        """If more than N levels, only top N are returned."""
        ob = {float(i): (1.0, 1) for i in range(20)}
        result = extract_top_n(ob, 5, reverse=False)
        assert len(result) == 5
        assert result[0][0] == 0.0
        assert result[4][0] == 4.0

    def test_empty_orderbook(self):
        result = extract_top_n({}, 3, reverse=False)
        assert result == [(None, None, None)] * 3


# ---------------------------------------------------------------------------
# Unit tests: build_row
# ---------------------------------------------------------------------------

class TestBuildRow:
    def test_columns(self):
        asks = {100.0: (1.5, 2)}
        bids = {99.0: (3.0, 1)}
        row = build_row(1000000, asks, bids)
        assert row["ts"] == 1000000
        assert row["a1_price"] == 100.0
        assert row["a1_qty"] == 1.5
        assert row["a1_ordcnt"] == 2.0
        assert row["b1_price"] == 99.0
        assert row["b1_qty"] == 3.0
        assert row["b1_ordcnt"] == 1.0
        # Padded levels
        assert row["a2_price"] is None
        assert row["b2_price"] is None

    def test_61_columns(self):
        """1 ts + 30 ask cols + 30 bid cols = 61."""
        row = build_row(0, {}, {})
        assert len(row) == 61


# ---------------------------------------------------------------------------
# Integration test: convert_file
# ---------------------------------------------------------------------------

class TestConvertFile:
    def test_single_snapshot(self):
        """Snapshot should produce one output row."""
        lines = [
            make_snapshot_line(
                [["100", "5", "2"], ["101", "3", "1"]],
                [["99", "10", "3"], ["98", "7", "2"]],
            ),
        ]
        input_path = write_tmp(lines)
        output_path = input_path.replace(".data", ".parquet")
        try:
            convert_file(input_path, output_path)
            table = pq.read_table(output_path)
            assert table.num_rows == 1
            assert table.column("a1_price")[0].as_py() == 100.0
            assert table.column("b1_price")[0].as_py() == 99.0
        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_snapshot_then_update(self):
        """Update should modify the orderbook correctly."""
        lines = [
            make_snapshot_line(
                [["100", "5", "2"], ["101", "3", "1"]],
                [["99", "10", "3"], ["98", "7", "2"]],
                ts="1000",
            ),
            make_update_line(
                [["100", "0", "0"], ["102", "8", "1"]],
                [["99", "15", "4"]],
                ts="1010",
            ),
        ]
        input_path = write_tmp(lines)
        output_path = input_path.replace(".data", ".parquet")
        try:
            convert_file(input_path, output_path)
            table = pq.read_table(output_path)
            assert table.num_rows == 2

            # After update: 100 deleted, 102 inserted
            row1 = table.slice(1, 1)
            assert row1.column("a1_price")[0].as_py() == 101.0
            assert row1.column("a2_price")[0].as_py() == 102.0
            # Bid 99 updated qty
            assert row1.column("b1_qty")[0].as_py() == 15.0
        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_multiple_snapshots(self):
        """Second snapshot should replace the orderbook entirely."""
        lines = [
            make_snapshot_line(
                [["100", "5", "2"]],
                [["99", "10", "3"]],
                ts="1000",
            ),
            make_update_line(
                [["101", "3", "1"]],
                [],
                ts="1010",
            ),
            make_snapshot_line(
                [["200", "1", "1"]],
                [["199", "2", "1"]],
                ts="2000",
            ),
        ]
        input_path = write_tmp(lines)
        output_path = input_path.replace(".data", ".parquet")
        try:
            convert_file(input_path, output_path)
            table = pq.read_table(output_path)
            assert table.num_rows == 3

            # Third row (second snapshot): completely replaced
            row2 = table.slice(2, 1)
            assert row2.column("a1_price")[0].as_py() == 200.0
            assert row2.column("b1_price")[0].as_py() == 199.0
            # Old 100 and 101 should be gone
            assert row2.column("a2_price")[0].as_py() is None
        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_snapshot_with_zero_qty_filtered(self):
        """Snapshot levels with qty=0 should not appear in orderbook."""
        lines = [
            make_snapshot_line(
                [["100", "5", "2"], ["101", "0", "0"]],
                [["99", "10", "3"]],
            ),
        ]
        input_path = write_tmp(lines)
        output_path = input_path.replace(".data", ".parquet")
        try:
            convert_file(input_path, output_path)
            table = pq.read_table(output_path)
            assert table.column("a1_price")[0].as_py() == 100.0
            assert table.column("a2_price")[0].as_py() is None
        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_timestamps_preserved(self):
        """Output ts should match input ts."""
        lines = [
            make_snapshot_line([], [], ts="1234567890"),
            make_update_line([], [], ts="1234567900"),
        ]
        input_path = write_tmp(lines)
        output_path = input_path.replace(".data", ".parquet")
        try:
            convert_file(input_path, output_path)
            table = pq.read_table(output_path)
            ts_col = table.column("ts").to_pylist()
            assert ts_col == [1234567890, 1234567900]
        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_schema_correct(self):
        """Output should have exactly 61 columns with correct names."""
        lines = [make_snapshot_line([], [], ts="0")]
        input_path = write_tmp(lines)
        output_path = input_path.replace(".data", ".parquet")
        try:
            convert_file(input_path, output_path)
            table = pq.read_table(output_path)
            assert table.num_columns == 61
            assert table.column_names[0] == "ts"
            assert "a1_price" in table.column_names
            assert "a10_ordcnt" in table.column_names
            assert "b1_price" in table.column_names
            assert "b10_ordcnt" in table.column_names
        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)
