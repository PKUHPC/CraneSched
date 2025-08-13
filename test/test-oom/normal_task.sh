#!/usr/bin/env bash
set -euo pipefail

echo "normal_task.sh: starting infinite loop (pid $$)"

cleanup() {
	echo "normal_task.sh: received signal, exiting" >&2
	exit 0
}
trap cleanup SIGTERM SIGINT

while true; do
	# sleep to avoid busy-looping CPU
	sleep 60
done
