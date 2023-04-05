#!/bin/bash
echo "Start running Jepsen testing for $1 in $2 mode, with txn-num $3, concurrency $4 and max-txn-length $5"
lein run test-all -w rw \
--txn-num $3 \
--concurrency $4 \
--max-txn-length $5 \
--time-limit 600 \
-r 500 \
--node dummy-node \
--isolation snapshot-isolation \
--expected-consistency-model snapshot-isolation \
--nemesis none \
--existing-postgres \
--no-ssh \
--database $1 \
--tidb-mode $2
echo "Finish running Jepsen testing for $1 in $2 mode"