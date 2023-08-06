lein run test-all -w rw \
--max-writes-per-key 24 \
--txn-num 1000 \
--concurrency 20 \
--max-txn-length 8 \
--time-limit 600 \
-r 1000 \
--node dummy-node \
--isolation snapshot-isolation \
--expected-consistency-model snapshot-isolation \
--time-limit 60 \
--nemesis none \
--existing-postgres \
--no-ssh \
--database dgraph \
--dbcop-workload-path /tmp/generate/hist-00000.json \
--dbcop-workload

