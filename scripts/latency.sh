#!/bin/bash

# Script for testing the latency of Crane commands

# Testcase directory
pushd crane || exit 1

commands=("cinfo" "cqueue" "cacct" "ccontrol show nodes" "cacctmgr list user")
for cmd in "${commands[@]}"
do
    echo "Testing execution time for command: $cmd"
    time $cmd > /dev/null
    echo "----------------------------------------"
done

# Test cbatch/cancel by submitting/cancelling a sample job
echo "Testing execution time for command: cbatch"
time cbatch_output=$(cbatch test.job)

task_id=$(echo "$cbatch_output" | grep -oP '(?<=Task Id allocated: )\d+')

echo "Job ID is $task_id"
echo "----------------------------------------"

echo "Testing execution time for command: ccancel"
time ccancel "$task_id"
echo "----------------------------------------"

echo "All tests completed."
popd || exit 1
