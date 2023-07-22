import subprocess
import time
import os
import shutil
import mysql.connector
import warnings
import platform

from log_parser import get_write_info_from_log, save2json

os_type = platform.system()

if os_type == "Darwin":
    base_dir = '/Users/ouyanghongrong/github-projects/disalg.dbcdc'
elif os_type == "Linux":
    base_dir = '/data/home/tsunaouyang/github-projects/dbcdc-runner'

txn_num_options = [1000, 2000, 3000, 4000, 5000]
concurrency_options = [3, 6, 9, 12, 15]
max_txn_len_options = [4, 8, 12, 16, 20]

default_txn_num = 3000
default_concurrency = 9
default_max_txn_len = 12


def prepare():
    logfile = base_dir + '/tidb/cdc.log'
    if os.path.exists(logfile):
        os.remove(logfile)


def write_into_tidb(idx):
    warnings.warn("write_into_tidb() is deprecated.")
    print("[BEGIN] Write into TiDB")

    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        database="test",
        port=4000
    )
    try:
        cur = conn.cursor()
        # 把所有行的值更新为 idx
        sql = "UPDATE notice SET v = {} WHERE v = {}".format(idx + 1, idx)
        print("Execute ", sql)
        cur.execute(sql)
        conn.commit()
        print(cur.rowcount)
        cur.close()
    except Exception as e:
        print(e)
    finally:
        conn.close()

    print("[FINISH] Write into TiDB")


def wait_until_mysql(idx):
    warnings.warn("wait_until_mysql() is deprecated.")
    print("[BEGIN] Wait Until MySQL")

    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456",
        database="test",
    )
    start_time = time.time()

    try:
        while True:
            cur = conn.cursor()
            try:
                query = "SELECT COUNT(*) FROM notice WHERE v = {}".format(idx + 1)
                cur.execute(query)
                wait_time = time.time() - start_time
                print("wait for table notice finished for {} seconds".format(wait_time))
                result = cur.fetchone()
                print(result)
                if result and result[0] == 10:
                    print("notice table has been finished")
                    break

                time.sleep(3)
            except mysql.connector.errors.ProgrammingError as e:
                print(e)
                time.sleep(3)
            finally:
                cur.close()
                conn.commit()

            if time.time() > start_time + 180:
                print("Timeout: notice table does not exist")
                break
    except Exception as e:
        print(e)
    finally:
        conn.close()

    print("[FINISH] Wait Until MySQL")


def read_from_mysql(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM txn0 ORDER BY k")
    res = cur.fetchall()
    conn.commit()
    cur.close()
    return res


def read_tidb():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        database="test",
        port=4000
    )
    res = read_from_mysql(conn)
    conn.close()
    return res


def read_mysql():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="123456",
        database="test",
    )
    res = read_from_mysql(conn)
    conn.close()
    return res


def wait_until_sync():
    start_time = time.time()
    res1 = read_tidb()
    print("TiDB: ", res1)

    while True:
        res2 = read_mysql()
        print("MySQL: ", res2)
        if res1 == res2:
            break

        wait_time = time.time() - start_time
        print("wait for table notice finished for {} seconds".format(wait_time))
        time.sleep(5)

        if time.time() > start_time + 180:
            print("Timeout: notice table does not exist")
            break


def save_log2json():
    logfile = base_dir + '/tidb/cdc.log'
    outfile = base_dir + '/store/latest/cdc.json'
    outlog = base_dir + '/store/latest/cdc.log'
    print("Start parse {} and save to {}".format(logfile, outfile))
    write_infos = get_write_info_from_log(logfile)
    save2json(write_infos, outfile, 4)
    print("Finish save")

    if os.path.exists(logfile):
        shutil.move(logfile, outlog)


def run_one_round(mode="opt", txn_num=default_txn_num, concurrency=default_concurrency, max_txn_len=default_max_txn_len):
    print(f"Start testing TiDB {mode} mode")
    print(f"txn_num={txn_num}, con={concurrency}, max_txn_len={max_txn_len}")
    # Start a new process T-watch for watching
    with subprocess.Popen(["./watch.sh"]) as p_watch:
        # Run test in a new process T-test after process T-watch starting for seconds
        time.sleep(3)
        with subprocess.Popen(["../run-tidb.sh", "tidb", mode, str(txn_num), str(concurrency), str(max_txn_len)]) as p_test:
            # After the script in process T-test finished, sleep for seconds, close process T-watch
            p_test.wait()

        # write_into_tidb(idx)
        # wait_until_mysql(idx)
        wait_until_sync()

        p_watch.terminate()

    # Phase cdc.log to json
    save_log2json()


if __name__ == '__main__':
    print("Start testing")
    prepare()
    test_cnt = 200
    for i in range(0, test_cnt):
        for mode in ["opt", "pess"]:
            print("Test: TiDB {} mode in Round {}".format(mode, i))
            for txn_num in txn_num_options:
                run_one_round(mode, txn_num=txn_num)
            for concurrency in concurrency_options:
                run_one_round(mode, concurrency=concurrency)
            for max_txn_len in max_txn_len_options:
                run_one_round(mode, max_txn_len=max_txn_len)

            print("Finish this round")
    print("Finish testing")
