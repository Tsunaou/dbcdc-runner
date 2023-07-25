import os
import sys
from subprocess import PIPE
import subprocess

command_lein = "lein run test-all -w rw \
--txn-num 120000 \
--time-limit 43200 \
-r 10000 \
--node dummy-node \
--isolation snapshot-isolation \
--expected-consistency-model snapshot-isolation \
--nemesis none \
--existing-postgres \
--no-ssh \
--database dgraph \
--dbcop-workload-path ${dbcop-workload-path} \
--dbcop-workload"


def get_all_files_in_directory(directory_path):
    all_files = []
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        if os.path.isfile(file_path):
            all_files.append(file_path)
    return all_files


def create_directory_if_not_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    else:
        print(f"Directory '{directory_path}' already exists.")


def generate_bincode(mode, variable):
    destination_path = '/tmp/generate/' + mode + '/' + str(variable) + '/'
    create_directory_if_not_exists(destination_path)

    nnode = 50
    readp = 0.5
    nvar = 1000
    key_distrib = 'zipf'
    nhist = 10
    ntxn = 2000
    nevt = 15

    command = ['dbcop', 'generate', '-d', destination_path,
               '--nnode', str(nnode),
               '--nevt', str(variable),
               '--readp', str(readp),
               '--nvar', str(nvar),
               '--key_distrib', str(key_distrib),
               '--nhist', str(nhist),
               '--ntxn', str(int(ntxn))]
    try:
        result = subprocess.run(command, stdout=PIPE, stderr=PIPE)

        # print("命令输出：")
        # print(result.stdout)
        #
        # print("错误输出：")
        # print(result.stderr)
        #
        # print("返回码：", result.returncode)

    except subprocess.CalledProcessError as e:
        sys.exit(1)
        # print("命令执行出错，返回码：", e.returncode)
        # print("错误输出：", e.stderr)

    return destination_path


def convert_to_json(path):
    command = ['dbcop', 'convert', '-d', path, '--from', 'bincode']
    try:
        result = subprocess.run(command, stdout=PIPE, stderr=PIPE)

        # print("命令输出：")
        # print(result.stdout)

        # print("错误输出：")
        # print(result.stderr)

        # print("返回码：", result.returncode)

    except subprocess.CalledProcessError as e:
        sys.exit(1)
        # print("命令执行出错，返回码：", e.returncode)
        # print("错误输出：", e.stderr)


if __name__ == '__main__':

    mode = 'nevt'
    variables = [5, 15, 30, 50, 100]
    for variable in variables:
        generated_path = generate_bincode(mode, variable)
        convert_to_json(generated_path)
        for json_file in get_all_files_in_directory(generated_path):
            if not json_file.endswith(".json"):
                continue
            os.system(
                command_lein.replace("${dbcop-workload-path}", json_file))