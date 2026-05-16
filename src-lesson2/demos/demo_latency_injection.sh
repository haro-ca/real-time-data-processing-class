#!/usr/bin/env bash
# ============================================================
# Inject network latency between CockroachDB nodes to simulate
# cross-region deployment. Uses `tc` (traffic control) via a
# netshoot sidecar that shares each container's network namespace.
#
# Usage:
#   ./demos/demo_latency_injection.sh add 50     # add 50ms latency
#   ./demos/demo_latency_injection.sh remove      # remove latency
#
# After adding latency, re-run benchmarks to see the impact:
#   uv run python run_all.py --rows 10000
#
# Expected: TPS craters. This makes the latency table real.
# ============================================================

set -euo pipefail

ACTION="${1:-add}"
DELAY_MS="${2:-50}"
CONTAINERS=("lesson2-crdb-1" "lesson2-crdb-2" "lesson2-crdb-3")

# Run a tc command inside a container's network namespace via netshoot.
# The CockroachDB image (RHEL 9 minimal) has no package manager and no tc;
# netshoot has tc pre-installed and shares the target network namespace.
tc_in() {
    local container="$1"
    shift
    docker run --rm \
        --network "container:${container}" \
        --cap-add NET_ADMIN \
        nicolaka/netshoot \
        tc "$@" 2>&1
}

add_latency() {
    echo "Adding ${DELAY_MS}ms latency to all inter-node traffic..."
    echo "  (simulating cross-region deployment)"
    echo ""
    for c in "${CONTAINERS[@]}"; do
        tc_in "$c" qdisc del dev eth0 root > /dev/null 2>&1 || true
        tc_in "$c" qdisc add dev eth0 root netem delay "${DELAY_MS}ms" \
            && echo "  ✓ ${c}: +${DELAY_MS}ms delay" \
            || echo "  ✗ ${c}: failed to inject latency"
    done
    echo ""
    echo "  Latency injected. Now re-run benchmarks:"
    echo "    uv run python run_all.py --rows 10000"
    echo ""
    echo "  Think about: if every COMMIT needs a Raft round-trip,"
    echo "  and each round-trip now costs ${DELAY_MS}ms extra,"
    echo "  what's your theoretical TPS ceiling per range?"
    echo "  → ~$((1000 / (DELAY_MS * 2))) TPS per range (at best)"
}

remove_latency() {
    echo "Removing injected latency..."
    for c in "${CONTAINERS[@]}"; do
        tc_in "$c" qdisc del dev eth0 root 2>/dev/null \
            && echo "  ✓ ${c}: latency removed" \
            || echo "  ✓ ${c}: nothing to remove"
    done
    echo ""
    echo "  Network restored to normal."
}

case "$ACTION" in
    add)    add_latency ;;
    remove) remove_latency ;;
    *)      echo "Usage: $0 {add|remove} [delay_ms]"; exit 1 ;;
esac
