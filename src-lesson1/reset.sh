#!/usr/bin/env bash
# Clean baseline between phases — truncate table, reset stats.
# Usage: ./reset.sh

set -euo pipefail

DSN="postgresql://bench:bench@localhost:5432/bench"

echo "Resetting bench database..."
psql "$DSN" <<SQL
  TRUNCATE orders RESTART IDENTITY;
  SELECT pg_stat_statements_reset();
  SELECT pg_stat_reset();
  SELECT pg_stat_reset_shared('bgwriter');
SQL

echo "✓ Table truncated, stats reset. Clean baseline ready."
