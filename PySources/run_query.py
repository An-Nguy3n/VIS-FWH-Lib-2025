import sys
import sqlite3


def run_query():
    print("Start execute query")
    db_path = sys.argv[1]
    with open(db_path.replace("f.db", "queries.bin"), "rb") as f:
        queries = f.read().decode("utf-8").split(";")

    connection = sqlite3.Connection(db_path)
    cursor = connection.cursor()
    cursor.execute("begin")
    for query in queries:
        if query.strip():
            cursor.execute(query)

    cursor.execute("commit")
    print("Done")


if __name__ == "__main__":
    run_query()
