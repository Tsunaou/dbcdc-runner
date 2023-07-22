import re
import json

# 定义正则表达式匹配日志格式
log_pattern = r"(.*)\[(.*)\] \[(.*):(.*)\] \[(.*)\] \[row-id=(.*)\] \[table-name=(.*)\] \[start-ts=(.*)\] \[commit-ts=(.*)\] \[columns=(.*)\] \[pre-columns=(.*)\]"


def save2json(input_dict, output_path, indent=0):
    with open(output_path, "w") as outfile:
        if indent == 0:
            json.dump(input_dict, outfile)
        else:
            json.dump(input_dict, outfile, indent=4)


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
    log_dict["row-id"] = int(match.group(6))
    log_dict["table-na,e"] = match.group(7)
    log_dict["start-ts"] = int(match.group(8))
    log_dict["commit-ts"] = int(match.group(9))

    columns = match.group(10)
    pre_columns = match.group(11)

    log_dict["columns"] = json.loads(columns.strip('\n\"').replace('\\', ''))
    log_dict["pre-columns"] = json.loads(pre_columns.strip('\n\"').replace('\\', ''))

    return log_dict


def get_write_info_from_log(logfile):
    write_info = {}

    # 遍历日志文件，提取每一行的信息，并将write的信息存储到字典write_info中
    with open(logfile, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if line.__contains__("notice") or line.__contains__("BOOLEAN"):
                continue
            row = extract_row(line)
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
            write_info[str(write)] = {
                'start_ts': start_ts, 'commit_ts': commit_ts, 'pre_write': pre_write}

    return write_info


if __name__ == '__main__':
    # store_dir = '/Users/ouyanghongrong/github-projects/disalg.dbcdc/store'
    # instance_dirs = ['/dbcdc rw tidb opt SI (SI) /latest']
    #
    store_dir = '/Users/ouyanghongrong/github-projects/disalg.dbcdc'
    instance_dirs = ['/tidb']

    for instance_dir in instance_dirs:
        base_dir = store_dir + instance_dir
        logfile = base_dir + '/cdc.log'
        outfile = base_dir + '/cdc.json'
        write_infos = get_write_info_from_log(logfile)
        save2json(write_infos, outfile, 4)
