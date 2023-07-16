import ast
import json

import requests

url = "http://175.27.241.31:8080/mutate?commitNow=true"
headers = {'Content-Type': 'application/json'}


def generate_txn(ops):
    query = "query { "
    query_count = 0
    mutations = []
    for op in ops:
        if op["t"] == "r":
            query_count += 1
            q = "get%d(func: uid(%d)) { value } " % (query_count, op["k"])
            query += q
        else:
            mutations.append({
                "set": {
                    "uid": op["k"],
                    "value": op["v"]
                }
            })
    query += "}"
    if len(mutations) == 0:
        mutations.append({
            "set": {
                "fake_data": 233
            }
        })
    
    print(query)
    print(mutations)
    
    return {
        "query": query,
        "mutations": mutations
    }


def send_request(req_data):
    return requests.post(url, headers=headers, json=req_data)


def fetch_ts(resp_content):
    resp_data = ast.literal_eval(resp_content.decode("utf-8"))
    ts = resp_data["extensions"]["txn"]
    return ts["start_ts"], ts["commit_ts"]


def main():
    operations = [{"t": "w", "k": 745, "v": 697}, {"t": "w", "k": 853, "v": 74},
                  {"t": "r", "k": 853}, {"t": "r", "k": 745}]
    request_data = generate_txn(operations)
    response = send_request(request_data)
    start_ts, commit_ts = fetch_ts(response.content)
    txn = {
        "tid": "my unique id",
        "sid": "my session id",
        "ops": operations,
        "sts": {
            "p": start_ts,
            "l": 0
        },
        "cts": {
            "p": commit_ts,
            "l": 0
        }
    }
    print(json.dumps(txn, indent=4))


if __name__ == '__main__':
    main()

