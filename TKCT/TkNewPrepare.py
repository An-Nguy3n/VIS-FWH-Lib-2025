import sqlite3
import os
import pandas as pd
import numpy as np
from TKCT.mergeTableDB import create_table, get_table_names


def sample_data_with_rate(data: pd.DataFrame, rate: float) -> pd.DataFrame:
    n = int(np.ceil(len(data) * rate))
    return data.sample(n=n)


def remove_file(path: str):
    if os.path.exists(path):
        os.remove(path)


def fetch_table_names(cursor, filter_1525: bool) -> list:
    cursor.execute(get_table_names())
    tables = cursor.fetchall()
    if filter_1525:
        return [int(tb[0][2:]) for tb in tables if tb[0].startswith("TT")]
    return [int(tb[0][1:]) for tb in tables if tb[0].startswith("T") and not tb[0].startswith("TT")]


def build_temp_db_path(db_path: str, filter_1525: bool) -> str:
    suffix = "f_1525_temp.db" if filter_1525 else "f_temp.db"
    return db_path[:-8] + suffix


def filter_unique_profit_value(db_path: str, critical_col: str, target: int = 100_000, filter_1525: bool = False):
    assert db_path.endswith("f_new.db")

    db_temp = build_temp_db_path(db_path, filter_1525)
    remove_file(db_temp)

    conn_temp = sqlite3.connect(db_temp)
    cursor_temp = conn_temp.cursor()
    conn_origin = sqlite3.connect(db_path)
    cursor_origin = conn_origin.cursor()

    table_indices = fetch_table_names(cursor_origin, filter_1525)
    print(f"Working on: {db_temp}, Tables: {table_indices}")

    # Create tables in temporary database
    for idx in table_indices:
        cursor_temp.execute(create_table(idx, critical_col))
    conn_temp.commit()

    for idx in table_indices:
        table_name = f"TT{idx}" if filter_1525 else f"T{idx}"

        # Count number of rows
        cursor_origin.execute(f"SELECT COUNT(*) FROM {table_name};")
        num_rows = cursor_origin.fetchone()[0]
        print(f"{db_temp}: Table {table_name}, Rows: {num_rows}")

        sample_rate = min(float(target) / num_rows, 1.0)

        # Fetch data in chunks
        cursor_origin.execute(f"SELECT * FROM {table_name};")
        list_df = []
        while True:
            batch = cursor_origin.fetchmany(1_000_000)
            if not batch:
                break

            batch_df = pd.DataFrame(batch)
            batch_df["temp"] = batch_df[2].round(3)

            sampled_batch = batch_df.groupby("temp", group_keys=False).apply(
                lambda x: sample_data_with_rate(x, sample_rate)
            )
            list_df.append(sampled_batch)

        # Merge all sampled batches
        data = pd.concat(list_df, ignore_index=True)

        # Re-sample if needed
        final_sample_rate = min(float(target) / len(data), 1.0)
        final_sample = data.groupby("temp", group_keys=False).apply(
            lambda x: sample_data_with_rate(x, final_sample_rate)
        )

        # Prepare and insert into temp DB
        records_to_insert = final_sample.drop(columns=["temp"]).values.tolist()
        print(f"{db_temp}: Table {table_name}, Inserted: {len(records_to_insert)} rows")

        cursor_temp.executemany(f"INSERT INTO T{idx} VALUES (?, ?, ?);", records_to_insert)
        conn_temp.commit()

    conn_origin.close()
    conn_temp.close()
