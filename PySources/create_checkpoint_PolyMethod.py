import sys
import sqlite3


def create_checkpoint_PolyMethod():
    db_path = sys.argv[1]
    num_operand = int(sys.argv[2])
    connection = sqlite3.Connection(db_path)
    cursor = connection.cursor()
    cursor.execute("begin")
    query = f"create table checkpoint_{num_operand}(id integer not null, num_opr_per_grp integer not null,"
    query += ",".join([f"E{i} integer not null" for i in range(num_operand*2)]) + ")"
    cursor.execute(query)
    query = f"insert into checkpoint_{num_operand} values (0,1"
    query += ",0"*2*num_operand + ")"
    cursor.execute(query)
    cursor.execute("commit")


if __name__ == "__main__":
    create_checkpoint_PolyMethod()
