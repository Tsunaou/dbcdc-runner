#!/bin/bash
echo "Start running Jepsen testing for $1 in $2 mode"
lein run test-all -w rw \
--txn-num 3000 \
--concurrency 9 \
--max-txn-length 12 \
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