import re
import json

# 定义正则表达式匹配日志格式
log_pattern = r"\[(.*)\] \[(.*)\] \[(.*):(.*)\] \[(.*)\] \[(.*)\]"


# 从日志字符串中解析出 row 数据
def extract_row(log_str):
    # 使用正则表达式匹配日志格式
    match = re.match(log_pattern, log_str)
    if not match:
        return None

    # 解析出 row 数据
    log_dict = {}
    log_dict["time"] = match.group(1)
    log_dict["level"] = match.group(2)
    log_dict["filename"] = match.group(3)
    log_dict["lineno"] = match.group(4)
    log_dict["event_type"] = match.group(5)
    row_str = match.group(6)

    # 解析 row 数据
    row_str = row_str.split('row="')[1].strip("\n").replace('\\', '')
    row_str = row_str[:-1]  # 去掉最后一个 "]" 及其后面的内容
    row_dict = json.loads(row_str)
    log_dict["row"] = row_dict

    return log_dict


def get_write_info_from_log(logfile):
    write_info = {}

    # 遍历日志文件，提取每一行的信息，并将write的信息存储到字典write_info中
    with open(logfile, 'r') as f:
        lines = f.readlines()
        for line in lines:
            log_dict = extract_row(line)
            row = log_dict["row"]
            start_ts = row['start-ts']
            commit_ts = row['commit-ts']
            columns = row['columns']
            pre_columns = row['pre-columns']

            def parse_column(columns):
                if columns is None:
                    return None
                key = columns[0]
                assert key['name'] == 'k'
                val = columns[1]
                assert val['name'] == 'v'
                k = key['value']
                v = val['value']
                return {'k': k, 'v': v}

            write = parse_column(columns)
            pre_write = parse_column(pre_columns)

            # 将write的信息存储到字典write_info中
            write_info[str(write)] = {'start_ts': start_ts, 'commit_ts': commit_ts, 'pre_write': pre_write}

    return write_info

if __name__ == '__main__':
    store_dir = '/Users/ouyanghongrong/github-projects/disalg.dbcdc/store'
    base_dir = store_dir + '/dbcdc rw tidb pess SI (SI) /20230328T112331.000+0800'
    logfile = base_dir + '/cdc-out.log'

    write_infos = get_write_info_from_log(logfile)

