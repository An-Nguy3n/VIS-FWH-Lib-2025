import sys
import sqlite3


def create_table_PolyMethod():
    db_path = sys.argv[1]
    start_cycle = int(sys.argv[2])
    end_cycle = int(sys.argv[3])
    num_operand = int(sys.argv[4])
    list_column = [_ for _ in sys.argv[5:]]

    connection = sqlite3.Connection(db_path)
    cursor = connection.cursor()
    cursor.execute("begin")
    for cycle in range(start_cycle, end_cycle+1):
        query = f"create table if not exists T{cycle}_{num_operand}(id integer not null,"
        query += "".join([f"E{i} integer not null," for i in range(num_operand)])
        query += ",".join([f"{c} real" for c in list_column]) + ")"
        cursor.execute(query)

    cursor.execute("commit")


if __name__ == "__main__":
    create_table_PolyMethod()
