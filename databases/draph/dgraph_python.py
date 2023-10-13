import pydgraph

def create_client_stub():
    return pydgraph.DgraphClientStub('175.27.241.31:9080')

def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)

def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))


def generate_txn(client, ops):
    # query = "query { "
    query_count = 0
    txn = client.txn()
    mutations = []
    for op in ops:
        if op["t"] == "r":
            query_count += 1
            query = "get%d(func: uid(%d)) { value } " % (query_count, op["k"])
        else:
            nquad = """<%d> <value> "%d" .""" % (op["k"], op["v"])
    mutation = txn.create_mutation(set_nquads=nquad)
    request = txn.create_request(query=query, mutations=[mutation], commit_now=True)

    return get_ts(txn.do_request(request))


def get_ts(response):
    response_text = str(response)
    text_list = response_text.split("\n")
    start_ts = text_list[2].split(":")[1].strip()
    commit_ts = text_list[3].split(":")[1].strip()
    return start_ts, commit_ts


def create_data(client, op):
    txn = client.txn()
    try:
        p = {
            'uid': op["k"],
            'value': op["v"]
        }

        response = txn.mutate(set_obj=p)
        print(response)

        commit_response = txn.commit()
        print(commit_response)

    finally:
        txn.discard()

