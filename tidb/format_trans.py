import json
from pprint import pprint
from transaction import Transaction, Operation, HLC, transaction_to_dict

def test_txn():
    txn = Transaction(
        cts=HLC(l=9, p=2),
        ops=[
            Operation(
                t='w',
                k=8,
                v=1
            ),
            Operation(
                t='r',
                k=8,
                v=1
            )
        ],
        sid='1',
        sts=HLC(l=4, p=2),
        tid='x'
    )

def load_tidb_history(cdc_json_path, hist_edn_path):
    pass

def load_template_history():
    template_json_path = "template/history-template.json"
    template_history = read_json(template_json_path)
    return template_history

def read_json(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)
        return data


if __name__ == '__main__':
    cdc_json_path = './template/cdc-template.json'
    hist_edn_path = './template/history-template.edn'

    cdc_data = read_json(cdc_json_path)

