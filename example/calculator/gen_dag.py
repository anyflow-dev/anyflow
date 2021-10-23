
# 1 2 3 4 5 6 7 8
# 9 10 11 12
# 13 14
# 15

# 15 14 13 12 11 10 9 8
# 7 6 5 4
# 3 2
# 1

import json
my_dag = []
TPL = "{}_node"


def get_level(num):
    i = 1
    while num >= pow(2, i):
        i += 1
    # print(i-1, num)
    return i-1


def gen_deps(num):
    level = get_level(num)
    return [
        TPL.format(num * 2),
        TPL.format(num * 2 + 1)
    ]


def gen_dag(dag_size):
    my_dag = [{
        "name": TPL.format(i),
        "node": "calc",
        "deps": [] if i >= dag_size / 2 else gen_deps(i),
        "params": {
            "val": i
        },
    } for i in range(1, dag_size)]
    return my_dag


if __name__ == "__main__":
    dag = json.dumps({
        "nodes": gen_dag(16)
    }, indent=4)
    with open("dag.json", "w") as f:
        f.write(dag)
    print(dag)
    # gen_dag(16)
