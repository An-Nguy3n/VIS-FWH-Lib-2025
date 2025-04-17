import sqlite3
import os
import numpy as np
import gc

# Constants
NUM_FML_PROCESS = [0, 70, 3710, 114380, 2443280, 28126028]


def decode(lst):
    return "_".join(map(str, lst))


def query_table(table_num, num_opr, critical_col):
    cols = ["id"] + [f"E{x}" for x in range(num_opr)] + [critical_col]
    return f"SELECT {','.join(cols)} FROM T{table_num}_{num_opr};"


def create_table(table_num, critical_col):
    return f"CREATE TABLE IF NOT EXISTS T{table_num} (id INTEGER, Formula TEXT, {critical_col} REAL);"


def get_table_names():
    return 'SELECT name FROM sqlite_master WHERE type = "table";'


def remove_file(path):
    if os.path.exists(path):
        os.remove(path)


def merge_table(db_path: str, critical_col: str):
    assert db_path.endswith(".db")
    new_db_path = db_path[:-3] + "_new.db"
    remove_file(new_db_path)

    conn_origin = sqlite3.connect(db_path)
    conn_new = sqlite3.connect(new_db_path)
    cur_origin = conn_origin.cursor()
    cur_new = conn_new.cursor()

    # Fetch all table names
    cur_origin.execute(get_table_names())
    table_names = [t[0] for t in cur_origin.fetchall() if t[0].startswith("T")]

    # Extract unique table numbers
    table_nums = np.unique([int(name.split("_")[0][1:]) for name in table_names])

    # Process each main table
    for table_num in table_nums:
        cur_new.execute(create_table(table_num, critical_col))
        conn_new.commit()

        sub_tables = [name for name in table_names if name.startswith(f"T{table_num}_")]
        for sub_table in sub_tables:
            num_opr = int(sub_table.split("_")[1])
            cur_origin.execute(query_table(table_num, num_opr, critical_col))

            bias = sum(NUM_FML_PROCESS[:num_opr])
            inserted_rows = 0

            while True:
                rows = cur_origin.fetchmany(10_000_000)
                if not rows:
                    break

                data_to_insert = [
                    (row[0] + bias, decode(row[1:-1]), row[-1]) for row in rows
                ]
                cur_new.executemany(
                    f"INSERT INTO T{table_num} VALUES (?, ?, ?);", data_to_insert
                )
                conn_new.commit()

                inserted_rows += len(rows)
                print(f"{new_db_path}: Table T{table_num}, {num_opr} operators, {inserted_rows} rows inserted")

                rows = None
                data_to_insert = None
                gc.collect()

    conn_origin.close()
    conn_new.close()
