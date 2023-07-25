../../dbcop generate -e 15 -n 50 -t 120 -v 1000 --key_distrib zipf -d /tmp/generate/
../../dbcop convert -d /tmp/generate/ --from bincode

echo "start time: " >> time.txt
date "+%Y-%m-%d %H:%M:%S" >> time.txt

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

echo "finish time: " >> time.txt
date "+%Y-%m-%d %H:%M:%S" >> time.txt
echo -e >> time.txt

python mongodb.py test.dbcdc11 "/media/njuselhx/Data/White-box-SI-Checking/dbcdc-runner/store/latest/history.edn"
