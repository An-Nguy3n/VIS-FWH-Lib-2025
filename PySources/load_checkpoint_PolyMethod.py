import sys
import os
import sqlite3
import numpy as np


def load_checkpoint_PolyMethod():
    db_path = sys.argv[1]
    folder_save = db_path.replace("/f.db", "")
    os.makedirs(f"{folder_save}/InputData/", exist_ok=True)
    try:
        os.remove(f"{folder_save}/InputData/checkpoint.bin")
    except: pass

    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    cursor.execute("select name from sqlite_master where type = 'table'")
    list_table = [_[0] for _ in cursor.fetchall() if _[0].startswith("checkpoint_")]
    if len(list_table) == 0:
        current = [
            np.zeros(2, np.int64),
            np.int64(1),
            np.int64(0)
        ]
        cursor.execute("begin")
        cursor.execute("create table checkpoint_1(id integer not null, num_opr_per_grp integer not null, E0 integer not null, E1 integer not null)")
        cursor.execute("insert into checkpoint_1 values (0, 1, 0, 0)")
        cursor.execute("commit")
    else:
        list_num = [int(_.replace("checkpoint_", "")) for _ in list_table]
        table_name = f"checkpoint_{max(list_num)}"
        cursor.execute(f"select * from {table_name}")
        info = cursor.fetchall()[0]
        current = [
            np.array(info[2:], np.int64),
            np.int64(info[1]),
            np.int64(info[0])
        ]

    with open(f"{folder_save}/InputData/checkpoint.bin", "wb") as f:
        f.write(np.int64(len(current[0])+2).tobytes())
        f.write(current[0].tobytes())
        f.write(current[1].tobytes())
        f.write(current[2].tobytes())

    connection.close()


if __name__ == "__main__":
    load_checkpoint_PolyMethod()
