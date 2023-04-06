#!/bin/bash

for txn_num in 1000 2000 3000 4000 5000
do

lein run test-all -w rw \
--txn-num ${txn_num} \
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
--database postgresql \
--key-count 10 \
--max-writes-per-key 128 \
--varchar-table

java -jar yb-txn-parser.jar \
-logDir /home/njuselhx/yugabyte-data/node-1/disk-1/yb-data/tserver/logs \
-linePrefix \#\#\# \
-tableName txn0 \
-walDir /home/njuselhx/yugabyte-data/node-1/disk-1/yb-data/tserver/wals \
-logDumpPath /home/njuselhx/yugabyte-2.17.0.0/bin/log-dump \
-storeExecDir "/media/njuselhx/Data/White-box-SI-Checking/dbcdc-runner/store/dbcdc rw postgresql num ${txn_num} con 9 len 12 SI (SI) /"

done