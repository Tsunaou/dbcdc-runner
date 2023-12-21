import os
import subprocess

store_example_path = "/Users/seedoilz/Downloads/dbcdc rw dgraph num 120000 con 1 len 12 SI (SI) "

for instance in os.listdir(store_example_path):
    if instance == 'latest' or instance == '.DS_Store':
        continue
    instance_path = os.path.join(store_example_path, instance)
    history_json_path = os.path.join(instance_path, 'history.json')

    command = f"java -jar /Users/seedoilz/Codes/TimeKiller/target/TimeKiller-jar-with-dependencies.jar --history_path {history_json_path} --initial_value 0"
    result = subprocess.run(command)
    print(result.stdout)


