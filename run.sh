#!/bin/bash
lein run test-all -w rw \
--max-writes-per-key 4 \
--concurrency 50 \
-r 500 \
--node dummy-node \
--isolation serializable \
--expected-consistency-model serializable \
--time-limit 10000 \
--nemesis none \
--existing-postgres \
--no-ssh \
--database postgresql \
# --varchar-table
