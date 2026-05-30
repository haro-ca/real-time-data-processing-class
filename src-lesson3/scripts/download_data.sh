#!/usr/bin/env bash
# Download NYC Taxi Yellow Q1 2025 Parquet files (~10M rows total).
#
# 10M is the base-10 "feel it" size — fits comfortably on a 16 GB laptop with
# the whole containerized stack running, but big enough to make a row store
# visibly suffer on a full scan. Scale ladder for mental anchoring:
#
#   10M   → this script (workshop default)
#   100M  → set MONTHS_PER_YEAR=12 YEARS="2023 2024 2025" below, or:
#               ./download_data.sh stretch
#   1B    → see download_data_1B.sh

set -euo pipefail

DATA_DIR="$(dirname "$0")/data"
mkdir -p "$DATA_DIR"

BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

# Default workshop scale: first 3 months of 2025 ≈ 10M rows
YEARS=(2025)
MONTHS=(01 02 03)

# Convenience override: `./download_data.sh stretch` → full 2023-2025
if [ "${1:-}" = "stretch" ]; then
    echo "Stretch mode: 2023-2025 full year (~128M rows, ~2 GB)"
    YEARS=(2023 2024 2025)
    MONTHS=(01 02 03 04 05 06 07 08 09 10 11 12)
fi

echo "Downloading Yellow Taxi data..."
for year in "${YEARS[@]}"; do
    for month in "${MONTHS[@]}"; do
        FILE="yellow_tripdata_${year}-${month}.parquet"
        if [ -f "$DATA_DIR/$FILE" ]; then
            echo "  = $FILE"
        else
            echo "  + $FILE"
            curl -sL "$BASE_URL/$FILE" -o "$DATA_DIR/$FILE"
        fi
    done
done

echo ""
echo "Files in $DATA_DIR/:"
ls -lh "$DATA_DIR"/*.parquet | awk '{print "  " $5 "\t" $NF}'
echo ""
echo "Total rows:"
echo "  ./bench python -c \"import duckdb; print(f'{duckdb.sql(\\\"SELECT COUNT(*) FROM '/workspace/data/yellow_tripdata_*.parquet'\\\").fetchone()[0]:,}')\""
