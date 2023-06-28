from collections import namedtuple

HLC = namedtuple('HLC', ['l', 'p'])
Operation = namedtuple('Operation', ['k', 't', 'v'])
Transaction = namedtuple("Transaction", ['cts', 'ops', 'sid', 'sts', 'tid'])

def transaction_to_dict(transaction):
    result = {}
    for field in transaction._fields:
        value = getattr(transaction, field)
        if isinstance(value, tuple):
            # result[field] = dict(zip(value._fields, value))
            result[field] = value._asdict()
        elif isinstance(value, list):
            result[field] = [operation._asdict() for operation in value]
        else:
            result[field] = value
    return result
