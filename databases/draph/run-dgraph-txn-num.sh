

for txn_num in 1000 2000 3000 4000 5000
do

lein run test-all -w rw \
--max-writes-per-key 24 \
--txn-num ${txn_num} \
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
--database dgraph

python dgraph_edn_to_json.py ${txn_num}

curl --location 'http://175.27.241.31:8080/alter' \
--header 'Content-Type: application/json' \
--data '{"drop_all": true}'

done
