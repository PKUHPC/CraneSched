#!/bin/bash
# CraneSched stress test for tracing performance analysis.
# Submits many short jobs in parallel, then analyzes with --system mode.
#
# Usage:
#   ./stress_test.sh [NUM_JOBS] [NODES_PER_JOB]
#   ./stress_test.sh 50 1        # 50 single-node jobs
#   ./stress_test.sh 20 2        # 20 two-node jobs

set -euo pipefail

NUM_JOBS=${1:-30}
NODES=${2:-1}
SCRIPT=$(mktemp /tmp/stress_XXXXXX.sh)
TRACE_SCRIPT="$(dirname "$0")/query_trace.py"

cat > "$SCRIPT" << 'EOF'
#!/bin/bash
sleep 2
EOF
chmod +x "$SCRIPT"

echo "=== CraneSched Stress Test ==="
echo "Jobs: $NUM_JOBS, Nodes/job: $NODES"
echo "Submitting..."

START_TIME=$(date +%s)

pids=()
for i in $(seq 1 "$NUM_JOBS"); do
  cbatch -N "$NODES" -t 00:01:00 -c 1 -o /dev/null -e /dev/null "$SCRIPT" &
  pids+=($!)
done

# Wait for all cbatch commands to return
for pid in "${pids[@]}"; do
  wait "$pid" 2>/dev/null || true
done

SUBMIT_TIME=$(( $(date +%s) - START_TIME ))
echo "All $NUM_JOBS jobs submitted in ${SUBMIT_TIME}s"

# Wait for jobs to complete
echo "Waiting for jobs to complete..."
while true; do
  running=$(cqueue 2>/dev/null | tail -n +2 | wc -l)
  [ "$running" -eq 0 ] && break
  echo "  $running jobs still running..."
  sleep 3
done

TOTAL_TIME=$(( $(date +%s) - START_TIME ))
echo "All jobs completed in ${TOTAL_TIME}s"

# Wait for span flush (BatchSpanProcessor: 5s delay)
echo "Waiting 20s for trace data flush..."
sleep 20

# Run system analysis
echo ""
echo "=== System Performance Analysis ==="
MINUTES=$(( (TOTAL_TIME / 60) + 2 ))
python3 "$TRACE_SCRIPT" --system --minutes "$MINUTES" --limit 10000

# Export Chrome Trace
OUTPUT="/tmp/stress_${NUM_JOBS}jobs.json"
python3 "$TRACE_SCRIPT" --system --minutes "$MINUTES" --limit 10000 --chrome "$OUTPUT"
echo ""
echo "Chrome Trace: $OUTPUT"

rm -f "$SCRIPT"
