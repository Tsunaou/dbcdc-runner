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
kInfo = Keyword('info')
kFunc = Keyword('f')

kValue = Keyword('value')
kRead = Keyword('r')
kWrite = Keyword('w')

kTime = Keyword('time')
kProcess = Keyword('process')
kIndex = Keyword('index')


def load_history(history_edn):
    with open(history_edn, 'r') as f:
        his_edn_data = f.readlines()

    return his_edn_data


def read_json(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)
        return data


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
            for raw_op in raw_value:  # type: ImmutableList
                if raw_op[0] == kWrite:
                    op = Operation(t='w', k=raw_op[1], v=raw_op[2])
                    if op in failed_key_value_pairs:
                        logger.warning(f"{op} is added before")
                    failed_key_value_pairs.add(op)
            log_progress(i, progress_step, hist_data_len)
            continue

        session = raw_txn.get(kProcess)
        read_only_flag = True

        txn = Transaction(cts=None, ops=[], sid=session, sts=None, tid=next(tid))

        for raw_op in raw_value:  # type: ImmutableList
            raw_op_type = raw_op[0]
            if raw_op_type == kWrite:
                read_only_flag = False
                op = Operation(t='w', k=raw_op[1], v=raw_op[2])
                txn.ops.append(op)
                op2tid[op] = idx
            elif raw_op_type == kRead:
                op = Operation(t='r', k=raw_op[1], v=raw_op[2] if raw_op is not None else 0)
                txn.ops.append(op)
            else:
                logger.warning(f"Invalid op type of {raw_op_type}")

        # TODO: 目前暂时不考虑只读事务
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


def phase_history(hist_edn_path, cdc_json_path):
    history, first_txn_on_each_process, failed_key_value_pairs, op2tid = phase_edn(hist_edn_path)

    logger.info(f"Start phasing {cdc_json_path}")
    cdc_data: dict = read_json(cdc_json_path)

    base_start_ts = float('inf')
    base_commit_ts = float('inf')

    for txn in first_txn_on_each_process.values():
        w = None
        for op in reversed(txn.ops):  # type: Operation
            if op.t == 'w':
                w = op
                break
        if w is None:
            logger.error(f"{txn} is read only and invalid")
            sys.exit(-1)

        kv_pair = "{'k': %d, 'v': %d}" % (w.k, w.v)
        ts: dict = cdc_data.get(kv_pair)
        start_ts = ts.get('start_ts')
        commit_ts = ts.get('commit_ts')
        base_start_ts = min(start_ts, base_start_ts)
        base_commit_ts = min(commit_ts, base_commit_ts)

    for kv_pair, ts in cdc_data.items():
        kv: dict = eval(kv_pair)
        key = kv.get('k')
        value = kv.get('v')
        start_ts = ts.get('start_ts')
        commit_ts = ts.get('commit_ts')

        if commit_ts < base_start_ts:
            logger.debug(f"{kv_pair} maybe in last round op so we deprecated it")
            continue

        op = Operation(t='w', k=key, v=value)
        if failed_key_value_pairs.__contains__(op):
            logger.debug(f"Invalid pairs {op} from failed transaction so deprecated")
            continue

        idx = op2tid.get(op)
        if idx is None:
            logger.debug(f"May be the last round op {op}, guess: {commit_ts < base_start_ts}")
            continue

        txn = history[idx]
        if txn.sts is None and txn.cts is None:
            history[idx] = txn._replace(sts=HLC(l=0, p=start_ts), cts=HLC(l=0, p=commit_ts))
        elif start_ts != txn.sts.p or commit_ts != txn.cts.p:
            logger.error(f"{txn} is inconsistent with start ts: {start_ts} and commit ts: {commit_ts}")
            raise Exception("Invalid Timestamp")

    logger.info("Finish phasing this history")
    return history


def example():
    hist_edn_template_path = './template/history-template.edn'
    cdc_json_template_path = './template/cdc-template.json'

    history = phase_history(hist_edn_template_path, cdc_json_template_path)

    output_buf = []
    for txn in history:
        if txn.cts is None or txn.sts is None:
            logger.warning(f"{txn} is invalid")
            continue
        output_buf.append(transaction_to_dict(txn))

    save2json(output_buf, './template/new-history-template.json', 4)


if __name__ == '__main__':
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    store_example_path = "../store-base/dbcdc rw tidb opt SI (SI) "
    for instance in os.listdir(store_example_path):
        if instance == 'latest':
            continue
        instance_path = os.path.join(store_example_path, instance)
        history_edn_path = os.path.join(instance_path, 'history.edn')
        cdc_json_path = os.path.join(instance_path, 'cdc.json')
        history_json_path = os.path.join(instance_path, 'history.json')

        if all([os.path.exists(path) for path in [history_edn_path, cdc_json_path]]):
            logger.info(f"Phasing {instance}")

            history = phase_history(history_edn_path, cdc_json_path)
            output_buf = []
            for txn in history:
                if txn.cts is None or txn.sts is None:
                    logger.warning(f"{txn} is invalid")
                    continue
                output_buf.append(transaction_to_dict(txn))

            save2json(output_buf, history_json_path, 4)
