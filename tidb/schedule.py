import subprocess
import time
import os
import shutil
import mysql.connector

from tidb.parser import get_write_info_from_log, save2json

base_dir = '/Users/ouyanghongrong/github-projects/disalg.dbcdc'


def write_into_tidb(idx):
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
        sql = "UPDATE notice SET v = {} WHERE v = {}".format(idx+1, idx)
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
                cur.execute("SELECT COUNT(*) FROM notice WHERE v = {}".format(idx+1))
                print("wait for table notice finished for {} seconds".format(time.time() - start_time))
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


def run_one_round(idx, mode="opt"):
    # Start a new process T-watch for watching
    with subprocess.Popen(["./watch.sh"]) as p_watch:
        # Run test in a new process T-test after process T-watch starting for seconds
        time.sleep(3)
        with subprocess.Popen(["../run-tidb.sh", "tidb", mode]) as p_test:
            # After the script in process T-test finished, sleep for seconds, close process T-watch
            p_test.wait()

        write_into_tidb(idx)
        wait_until_mysql(idx)

        p_watch.terminate()

    # Phase cdc.log to json
    save_log2json()


if __name__ == '__main__':
    print("Start testing")
    for mode in ["opt", "pess"]:
        for i in range(0, 10):
            if mode == 'opt':
                idx = i
            else:
                idx = i+10
            print("Test: TiDB {} mode in Round {}".format(mode, idx))
            run_one_round(idx, mode)
            print("Finish this round")
    print("Finish testing")
