import json
import sys
import os
import time

import edn_format
from edn_format import Keyword
from pymongo import MongoClient

client = MongoClient(host="119.45.142.75", port=27018)
database = client.get_database('local')
collection = database.get_collection('oplog.rs')


def parse_log():
    res = []
    cmd = "sshpass -p hfWei@926 scp root@119.45.142.75:/root/mongo-config/mongo-srs.log ./server3.log"
    os.popen(cmd).read()
    with open('./server3.log') as f:
        log = f.readlines()
    cmd = "sshpass -p hfWei@926 scp root@175.27.169.129:/root/mongo-config/mongo-srs.log ./server2.log"
    os.popen(cmd).read()
    with open("./server2.log") as f:
        log.extend(f.readlines())
    cmd = "sshpass -p hfWei@926 scp root@175.27.241.31:/root/mongo-config/mongo-srs.log ./server1.log"
    os.popen(cmd).read()
    with open("./server1.log") as f:
        log.extend(f.readlines())
    for item in log:
        if item == '\n':
            continue
        item = json.loads(item)
        if item['c'] != 'TXN' or not item['ctx'].startswith('conn') or item['msg'] != 'transaction':
            continue
        session_id = item['attr']['parameters']['lsid']['id']['$uuid']
        txn_id = item['attr']['parameters']['txnNumber']
        start_ts = item['attr']['readTimestamp']
        res.append({
            "session_id": session_id,
            "txn_id": txn_id,
            "start_ts": start_ts
        })
    return res


def parse_oplog(name):
    res = []
    oplog = list(collection.find({'txnNumber': {'$exists': True},
                                  'lsid': {'$exists': True}, 'o.applyOps': {'$exists': True}}))
    for item in oplog:
        if item['o']['applyOps'][0]['ns'] != name:
            continue
        session_id = item['lsid']['id']
        txn_id = item['txnNumber']
        commit_ts = item['ts']
        res.append({
            "session_id": session_id,
            "txn_id": txn_id,
            "commit_ts": commit_ts
        })
    return res


def parse_edn(filepath):
    res = []
    with open(filepath) as f:
        edn = f.readlines()
    for item in edn:
        item = edn_format.loads(item)
        if item.get(Keyword('type')) != Keyword('ok'):
            continue
        ops = []
        operations = item.get(Keyword('value'))
        for operation in operations:
            if operation[2] is not None:
                op = {
                    "t": str(operation[0])[1:],
                    "k": operation[1],
                    "v": operation[2]
                }
            else:
                op = {
                    "t": str(operation[0])[1:],
                    "k": operation[1]
                }
            ops.append(op)
        session_id = item.get(Keyword('session-info')).get(Keyword('uuid'))
        txn_id = item.get(Keyword('session-info')).get(Keyword('txn-number'))
        res.append({
            "tid": txn_id,
            "sid": session_id,
            "ops": ops,
            "sts": None,
            "cts": None
        })
    return res


def merge(txns, txns_with_sts, txns_with_cts):
    for txn in txns:
        session_id = txn['sid']
        txn_id = txn['tid']
        for txn_with_sts in txns_with_sts:
            if txn_with_sts['session_id'] != session_id or txn_with_sts['txn_id'] != txn_id:
                continue
            idx1 = txn_with_sts['start_ts'].find('(') + 1
            idx2 = txn_with_sts['start_ts'].find(',', idx1)
            idx3 = idx2 + 2
            idx4 = txn_with_sts['start_ts'].find(')', idx3)
            txn['sts'] = {
                "p": int(txn_with_sts['start_ts'][idx1:idx2]),
                "l": int(txn_with_sts['start_ts'][idx3:idx4])
            }
            txns_with_sts.remove(txn_with_sts)
            break
        for txn_with_cts in txns_with_cts:
            if str(txn_with_cts['session_id']) != session_id or txn_with_cts['txn_id'] != txn_id:
                continue
            txn['cts'] = {
                "p": txn_with_cts['commit_ts'].time,
                "l": txn_with_cts['commit_ts'].inc
            }
            txns_with_cts.remove(txn_with_cts)
            break
        read_only = True
        for op in txn['ops']:
            if op['t'] == 'w':
                read_only = False
                break
        if read_only:
            txn['cts'] = txn['sts']
        assert txn['sts'] is not None
        assert txn['cts'] is not None
        txn['tid'] = session_id + '!' + str(txn_id)
    assert len(txns_with_cts) == 0
    return txns


def save(txns):
    with open(str(time.time()) + '.json', 'w') as f:
        json.dump(txns, f)


def clear_log():
    with open('./server3.log', "w") as f:
        f.write("")
    cmd = "sshpass -p hfWei@926 scp ./server3.log root@119.45.142.75:/root/mongo-config/mongo-srs.log"
    os.popen(cmd).read()
    with open("./server2.log", "w") as f:
        f.write("")
    cmd = "sshpass -p hfWei@926 scp ./server2.log root@175.27.169.129:/root/mongo-config/mongo-srs.log"
    os.popen(cmd).read()
    with open("./server1.log", "w") as f:
        f.write("")
    cmd = "sshpass -p hfWei@926 scp ./server1.log root@175.27.241.31:/root/mongo-config/mongo-srs.log"
    os.popen(cmd).read()


def main():
    txns_with_sts = parse_log()
    txns_with_cts = parse_oplog(sys.argv[1])
    txns = parse_edn(sys.argv[2])
    assert len(txns_with_sts) >= len(txns)
    assert len(txns_with_cts) <= len(txns)
    txns = merge(txns, txns_with_sts, txns_with_cts)
    save(txns)
    clear_log()


if __name__ == '__main__':
    main()
