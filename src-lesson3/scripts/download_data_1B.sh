#!/usr/bin/env bash
# Easter egg: download the full NYC Taxi Yellow archive 2009-2025 (~1.5B rows).
#
# WARNING:
#   - ~25 GB compressed Parquet on disk.
#   - Schema drifts heavily across years (CSV→Parquet transition in 2009-2010;
#     congestion_surcharge added 2019; airport_fee added 2022; cbd_congestion_fee
#     added 2025). The loader uses DuckDB's union_by_name=true to absorb this.
#   - Pre-2011 files used a different column naming scheme — DuckDB still reads
#     them but some columns will be NULL across the merged view.
#   - Postgres load will take ~1 hour and produce a ~250 GB table.
#
# Only run this if you actually want to feel what "scan a billion rows" costs.

set -euo pipefail

DATA_DIR="$(dirname "$0")/data"
mkdir -p "$DATA_DIR"

BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

echo "Downloading FULL Yellow Taxi archive (2009-2025)..."
echo "  ~25 GB compressed, ~1.5B rows total."
echo ""

for year in $(seq 2009 2025); do
    for month in $(seq -w 1 12); do
        FILE="yellow_tripdata_${year}-${month}.parquet"
        if [ -f "$DATA_DIR/$FILE" ]; then
            echo "  = $FILE"
        else
            echo "  + $FILE"
            # || true: some old months 404; keep going.
            curl -sLf "$BASE_URL/$FILE" -o "$DATA_DIR/$FILE" || \
                { rm -f "$DATA_DIR/$FILE"; echo "    (not available)"; }
        fi
    done
done

echo ""
echo "Done. Counting rows (this takes a minute)..."
uv run python -c "
import duckdb
n = duckdb.sql(\"SELECT COUNT(*) FROM read_parquet('$DATA_DIR/yellow_tripdata_*.parquet', union_by_name=true)\").fetchone()[0]
print(f'  Total: {n:,} rows')
"
