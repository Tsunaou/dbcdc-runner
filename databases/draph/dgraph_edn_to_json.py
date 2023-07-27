import json
import sys
import os
import edn_format
from edn_format import ImmutableDict, ImmutableList, Keyword
from transaction import Transaction, Operation, HLC, transaction_to_dict
from loguru import logger
from itertools import count
from schedule import save2json

kType = Keyword('type')
kOK = Keyword('ok')
kInvoke = Keyword('invoke')
kFail = Keyword('fail')
kValue = Keyword('value')
kRead = Keyword('r')
kWrite = Keyword('w')
kTs = Keyword('ts')
kProcess = Keyword('process')
kIndex = Keyword('index')
kRts = Keyword('rts')
kCts = Keyword('cts')

def read_json(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)
        return data

def load_history(history_edn):
    with open(history_edn, 'r') as f:
        his_edn_data = f.readlines()
    return his_edn_data

def log_progress(processed_count, progress_step, total_count):
    if processed_count % progress_step == 0:
        progress = processed_count / total_count * 100
        logger.info(f"Processed {progress:.0f}% of history.edn")

def phase_edn(hist_edn_path):
    logger.info(f"Start phasing {hist_edn_path}")
    hist_data = load_history(hist_edn_path)
    # 记录每个进程的状态
    concurrency = 50  # TODO: 暂时设置为 50，后期应该从代码里读
    history = []  # type: list[Transaction]
    first_txn_on_each_process = dict()
    failed_key_value_pairs = set()  # type: set[Operation]
    op2tid = dict()
    idx = 0
    tid = count()

    # 打印进度日志用
    hist_data_len = len(hist_data)
    progress_step = hist_data_len // 10  # 每 10% 打印一次进度日志

    for i, line in enumerate(hist_data):
        raw_txn = edn_format.loads(line)  # type: ImmutableDict
        raw_type = raw_txn.get(kType)

        # 首先排除所有类型是 :invoke 的项
        if raw_type == kInvoke:
            log_progress(i, progress_step, hist_data_len)
            continue

        raw_value = raw_txn.get(kValue)  # type: ImmutableList

        # 记录下中止事务的所有写操作
        if raw_type != kOK:
            # for raw_op in raw_value:  # type: ImmutableList
            #     if raw_op[0] == kWrite:
            #         op = Operation(t='w', k=raw_op[1], v=raw_op[2])
            #         if op in failed_key_value_pairs:
            #             logger.warning(f"{op} is added before")
            #         failed_key_value_pairs.add(op)
            # log_progress(i, progress_step, hist_data_len)
            continue

        session = raw_txn.get(kProcess)
        read_only_flag = True

        txn = Transaction(cts=None, ops=[], sid=session, sts=None, tid=raw_txn.get(kIndex))

        read_list = []
        write_list = []

        rawTimestamp = raw_txn.get(kTs)


        txn = txn._replace(sts=HLC(l=0, p=rawTimestamp.get(kRts)), cts=HLC(l=0, p=rawTimestamp.get(kCts)))

        for raw_op in raw_value:  # type: ImmutableList
            raw_op_type = raw_op[0]
            if raw_op_type == kWrite:
                read_only_flag = False
                op = Operation(t='w', k=raw_op[1], v=raw_op[2])
                # 放入写列表
                write_list.append(op)
                # TODO
                # 存疑 ？ idx 是什么
                op2tid[op] = idx
            elif raw_op_type == kRead:
                op = Operation(t='r', k=raw_op[1], v=raw_op[2] if raw_op is not None else 0)
                # 放入读列表
                read_list.append(op)
            else:
                logger.warning(f"Invalid op type of {raw_op_type}")

        # 先放读
        for read in read_list:
            txn.ops.append(read)
        for write in write_list:
            txn.ops.append(write)



        if read_only_flag:
            log_progress(i, progress_step, hist_data_len)
            continue

        history.append(txn)
        idx = idx + 1

        if len(first_txn_on_each_process.keys()) != concurrency:
            process = session % concurrency
            if first_txn_on_each_process.get(process) is None:
                first_txn_on_each_process[process] = txn

        log_progress(i, progress_step, hist_data_len)
        continue

    return history, first_txn_on_each_process, failed_key_value_pairs, op2tid


if __name__ == '__main__':
    # 测试用
    # history, first_txn_on_each_process, failed_key_value_pairs, op2tid = phase_edn('./template/history.edn')
    # output_buf = []
    # for txn in history:
    #     if txn.cts is None or txn.sts is None:
    #         logger.warning(f"{txn} is invalid")
    #         continue
    #     output_buf.append(transaction_to_dict(txn))

    # save2json(output_buf, './template/history.json', 4)

    logger.remove()
    logger.add(sys.stderr, level="INFO")
    
    if len(sys.argv) < 2:
        print("Usage: python dgraph_edn_to_json.py 10000(your txn number)")
        sys.exit(1)

    txn_num = str(sys.argv[1])

    store_example_path = "../../store/dbcdc rw dgraph num " + txn_num + " con 20 len 8 SI (SI) "

    for instance in os.listdir(store_example_path):
        if instance == 'latest':
            continue
        instance_path = os.path.join(store_example_path, instance)
        history_edn_path = os.path.join(instance_path, 'history.edn')
        history_json_path = os.path.join(instance_path, 'history.json')
    
        logger.info(f"Phasing {instance}")

        history, first_txn_on_each_process, failed_key_value_pairs, op2tid = phase_edn(history_edn_path)
        output_buf = []
        for txn in history:
            if txn.cts is None or txn.sts is None:
                logger.warning(f"{txn} is invalid")
                continue
            output_buf.append(transaction_to_dict(txn))

        save2json(output_buf, history_json_path, 4)