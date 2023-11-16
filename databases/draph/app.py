import re
import threading

from flask import Flask, jsonify, json, request
import logging
import pydgraph

app = Flask(__name__)

dgraph_server = "175.27.241.31:9080"

# logging.basicConfig(level=logging.DEBUG)

# 配置日志输出到文件
logging.basicConfig(filename='dgraph.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

connection_pool = {}

pool_index = 0

lock = threading.Lock()


# 释放链接接口
@app.route('/release_connection', methods=['POST'])
def release_connection():
    try:
        global request
        data = json.loads(request.data)  # 将json字符串转为dict
        session_id = data['session_id']
        # lock.acquire()
        global connection_pool
        del connection_pool[session_id]
        logging.info("删除链接 " + str(session_id))
        # lock.release()
        return jsonify({"result": "Success"})
    except:
        logging.error("删除链接失败")
        return jsonify({"result": "Failure", "error": "删除链接失败"})
        # raise Exception("删除链接失败")


# 处理创建新 Session 的请求
@app.route('/create_session', methods=['POST'])
def create_session():
    try:
        # lock.acquire()
        # 创建一个 Dgraph 连接并获取 Session ID
        client_stub = pydgraph.DgraphClientStub(dgraph_server)
        client = pydgraph.DgraphClient(client_stub)
        logging.info("创建client成功")

        global connection_pool
        global pool_index
        # 将客户端连接添加到连接池
        connection_pool[pool_index] = client
        pool_index += 1

        # 返回 Session ID
        session_id = len(connection_pool) - 1
        logging.info("client成功加入connection pool, session id为" + str(session_id))

        # lock.release()
        return jsonify({"result": "Success", "session_id": session_id})
    except:
        logging.error("创建session或添加到链接池失败")
        return jsonify({"result": "Failure", "error": "创建session或添加到链接池失败"})
        # raise Exception("创建会话失败")


@app.route('/commit_transaction', methods=['POST'])
def commit_transaction():
    global request
    data = json.loads(request.data)
    session_id = data['session_id']
    ops = data['ops']
    if session_id < 0 or session_id >= len(connection_pool):
        return jsonify({"result": "Failure", "error": "Invalid session ID"})
    # lock.acquire()
    client = connection_pool[session_id]
    query = "query { "
    query_count = 0
    txn = client.txn()
    mutations = []
    read_keys = []
    result = []
    for op in ops:
        if len(op) == 1:
            result.append("read")
            query_count += 1
            q = "get%d(func: uid(%d)) { value } " % (query_count, op[0] + 1)
            read_keys.append(op[0] + 1)
            query += q
        else:
            result.append(None)
            nquad = """<%d> <value> "%d" .""" % (op[0] + 1, op[1])
            mutation = txn.create_mutation(set_nquads=nquad)
            mutations.append(mutation)

    # 如果是只写事务那么可能需要读一个空的东西
    if len(read_keys) == 0:
        q = "get%d(func: uid(%d)) { value } " % (query_count, 1)
        query += q
    query += "}"
    dgraph_request = txn.create_request(query=query, mutations=mutations, commit_now=True)

    try:
        res = txn.do_request(dgraph_request)
    except:
        logging.error(f"处理失败：<{session_id}, {ops}")
        return jsonify({"result": "Failure", "error": "transaction has been aborted"})
    logging.info(f"<{session_id}, {ops}")

    # lock.release()

    logging.info("解析事务操作成功")

    # 只读事务
    if len(mutations) == 0:
        try:
            response_text = str(res)
            text_list = response_text.split("\n")
            start_ts = text_list[2].split(": ")[1].strip()

            read_data = str(res).split("\n")[0].strip("json: ")
            read_data = read_data.replace("\"", "")
            read_data = read_data.replace("\\", "")

            json_str = re.sub(r'([a-zA-Z0-9_]+):', r'"\1":', read_data)
            ret_dict = eval(json_str)

            # pattern = r'value:(\d+)'
            # matches = re.findall(pattern, read_data)
            # assert len(matches) == len(read_keys)
            pointer = 1
            for i in range(0, len(result)):
                if result[i] is None:
                    continue
                else:
                    if len(ret_dict["get" + str(pointer)]) == 0:
                        result[i] = 0
                    else:
                        result[i] = ret_dict["get" + str(pointer)][0]["value"]
                    pointer += 1

            # pattern = r'value:(\d+)'
            # matches = re.findall(pattern, read_data)
            # assert len(matches) == len(read_keys)
            # read_values = []
            # for i in range(0, len(matches)):
            #     read_values.append({read_keys[i]: matches[i]})

            logging.info("开始时间戳获得成功:" + start_ts)
            return jsonify({"result": "Success", "start_ts": start_ts, "values": result})
        except:
            logging.error("只读事务开始时间戳解析失败")
            return jsonify({"result": "Failure", "error": "只读事务开始时间戳解析失败"})
            # raise Exception("只读事务返回值处理错误")
        # print(start_ts)
    # 只写事务
    elif len(read_keys) == 0:
        try:
            start_ts, commit_ts = get_ts(res)

            logging.info("开始时间戳获得成功:" + start_ts + " commit时间戳获得成功:" + commit_ts)
            return jsonify({"result": "Success", "start_ts": start_ts, "commit_ts": commit_ts, "values": result})
        except:
            logging.error("只写事务时间戳解析失败")
            return jsonify({"result": "Failure", "error": "只写事务时间戳解析失败"})
            # raise Exception("读写事务返回值处理错误")
    else:
        try:
            start_ts, commit_ts = get_ts(res)

            read_data = str(res).split("\n")[0].strip("json: ")
            read_data = read_data.replace("\"", "")
            read_data = read_data.replace("\\", "")

            json_str = re.sub(r'([a-zA-Z0-9_]+):', r'"\1":', read_data)
            ret_dict = eval(json_str)

            # pattern = r'value:(\d+)'
            # matches = re.findall(pattern, read_data)
            # assert len(matches) == len(read_keys)
            pointer = 1
            for i in range(0, len(result)):
                if result[i] is None:
                    continue
                else:
                    if len(ret_dict["get" + str(pointer)]) == 0:
                        result[i] = 0
                    else:
                        result[i] = ret_dict["get" + str(pointer)][0]["value"]
                    pointer += 1
            logging.info("开始时间戳获得成功:" + start_ts + " commit时间戳获得成功:" + commit_ts)
            return jsonify({"result": "Success", "start_ts": start_ts, "commit_ts": commit_ts, "values": result})
        except:
            logging.error("读写事务时间戳解析失败")
            return jsonify({"result": "Failure", "error": "读写事务时间戳解析失败"})


@app.route('/drop_all', methods=['POST'])
def drop_all():
    global request
    try:
        data = json.loads(request.data)
        session_id = data['session_id']
        client = connection_pool[session_id]
        logging.info("数据库数据清空成功")
        client.alter(pydgraph.Operation(drop_all=True))
        return jsonify({"result": "Success"})
    except:
        logging.error("数据库清空失败")
        return jsonify({"result": "Failure", "error": "数据库清空失败"})
        # raise Exception("数据库清空失败")


def get_ts(response):
    response_text = str(response)
    text_list = response_text.split("\n")
    start_ts = text_list[2].split(": ")[1].strip()
    commit_ts = text_list[3].split(": ")[1].strip()
    return start_ts, commit_ts


if __name__ == '__main__':
    app.run(threaded=True)
