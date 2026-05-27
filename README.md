# BTC-USD-SWAP L2 Orderbook Converter

Converts OKX BTC-USD-SWAP L2 orderbook `.data` files to Parquet format, retaining only the top 10 bid/ask levels per timestamp.

## Dependencies

- Python >= 3.8
- pyarrow
- pytest (for testing)

```bash
pip install pyarrow pytest
```

## Usage

```bash
# Default: convert both day files to parquet_output/
python convert_to_parquet.py

# Specify input files
python convert_to_parquet.py my_data.data

# Custom output directory
python convert_to_parquet.py -o output_dir file1.data file2.data
```

Output parquet files are saved to `parquet_output/` by default, one `.parquet` per input `.data` file (same stem name).

## Output Format

Each row in the parquet file represents one orderbook moment (snapshot or update). Columns:

| Column | Type | Description |
|--------|------|-------------|
| `ts` | int64 | Timestamp in milliseconds |
| `a{1-10}_price` | float64 | Ask price at level 1-10 (ascending, level 1 = best/lowest ask) |
| `a{1-10}_qty` | float64 | Ask quantity at level 1-10 |
| `a{1-10}_ordcnt` | float64 | Number of orders at ask level 1-10 |
| `b{1-10}_price` | float64 | Bid price at level 1-10 (descending, level 1 = best/highest bid) |
| `b{1-10}_qty` | float64 | Bid quantity at level 1-10 |
| `b{1-10}_ordcnt` | float64 | Number of orders at bid level 1-10 |

Total: 61 columns per row.

## How It Works

1. Reads the `.data` file line-by-line (memory-efficient, no full-load).
2. Maintains an in-memory orderbook following OKX update rules:
   - **snapshot**: replaces the entire orderbook.
   - **update**: inserts new price levels, updates existing ones, or deletes levels with quantity=0.
3. For each line, extracts the top 10 ask levels (lowest price) and top 10 bid levels (highest price).
4. Writes output in batches of 50,000 rows to Parquet (snappy compression).

## Running Tests

```bash
pytest test_convert.py -v
```

Tests cover: line parsing, orderbook update logic, top-N extraction, row building, and end-to-end conversion with synthetic data.
