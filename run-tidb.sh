#!/bin/bash
echo "Start running Jepsen testing for $1 in $2 mode"
lein run test-all -w rw \
--max-writes-per-key 16 \
--concurrency 50 \
-r 500 \
--node dummy-node \
--isolation snapshot-isolation \
--expected-consistency-model snapshot-isolation \
--time-limit 60 \
--nemesis none \
--existing-postgres \
--no-ssh \
--database $1 \
--tidb-mode $2
echo "Finish running Jepsen testing for $1 in $2 mode"