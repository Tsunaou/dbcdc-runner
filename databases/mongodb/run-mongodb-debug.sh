lein run test-all -w rw \
--max-writes-per-key 5000 \
--txn-num 5000 \
--concurrency 50 \
--max-txn-length 15 \
--time-limit 1000000 \
-r 500 \
--node dummy-node \
--isolation snapshot-isolation \
--expected-consistency-model snapshot-isolation \
--nemesis none \
--existing-postgres \
--no-ssh \
--database mongodb \
--dbcop-workload-path /tmp/generate/hist-00000.json \
--dbcop-workload