#!/bin/bash

for i in {1..20}
do

lein run test-all -w rw \
--max-writes-per-key 24 \
--txn-num 500 \
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
--database postgresql

# java -jar yb-txn-parser.jar \
# -logDir /home/young/disk1/yb-data/tserver/logs \
# -linePrefix \#\#\# \
# -tableName txn0 \
# -walDir /home/young/disk1/yb-data/tserver/wals \
# -logDumpPath /home/young/yugabyte-2.17.1.0/bin/log-dump \
# -storeExecDir "/home/young/dbcdc-runner/store/dbcdc rw postgresql num 500 con 5 len 8 SI (SI) /"

done