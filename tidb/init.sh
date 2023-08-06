#!/bin/bash
tiup playground v6.5.1 --db 2 --pd 3 --kv 3 --tag dbcdc-diy
./bin/cdc server --pd http://127.0.0.1:2379 --data-dir /tmp/cdc_data --log-file /tmp/cdc_data/tmp.log
./bin/cdc cli changefeed create --pd http://127.0.0.1:2379 --sink-uri mysql://root:123456@localhost:3306/
