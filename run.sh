#!/bin/bash

lein run test-all -w rw \
--key-count 100 \
--max-writes-per-key 100 \
--concurrency 10 \
--rate 500 \
--time-limit 1440000 \
--txn-num 1000 \
--test-count 100
--max-txn-length 4 \
--nemesis none \
--existing-postgres \
--node dummy-node \
--isolation serializable \
--expected-consistency-model serializable \
--no-ssh \
--database postgresql
# --varchar-table