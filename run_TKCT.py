import os
import pandas as pd
import sqlite3
from multiprocessing import Pool, cpu_count
from TKCT.mergeTableDB import merge_table
from TKCT.filter_1525 import filter_1525
from TKCT.TkNewPrepare import filter_unique_profit_value
from TKCT.TKCT_old import filter as filter_old
from TKCT.TKCT_new import filter as filter_new
from TKCT.mergeTableDB import get_table_names
from PySources.base import Base


list_db_path = [
    ("/Users/nguyenhuuan/Downloads/Ngn1/f.db", "HarNgn1"),
    ("/Users/nguyenhuuan/Downloads/Ngn2/f.db", "HarNgn2"),
    ("/Users/nguyenhuuan/Downloads/Ngn3/f.db", "HarNgn3")
]

FOLDER = "/Users/nguyenhuuan/Downloads/FMLs"
DATA_PATH = "/Users/nguyenhuuan/Desktop/VIS-FWH-Lib-2025/DATA/HOSE_Field_2025_simulate_20250401.xlsx"
TARGET = 10000
NUM_FIELD = 35
LEVEL = 2
RATE = 0.21
EVAL_METHOD = "root"
YEAR_MIN = 2015
INTEREST = 1.06
VALUEARG_THRESHOLD = 5e8
EXCLUDE_THRESHOLD = 0.0


def run_task(args):
    task_name = args["name"]
    if task_name == "TK_old":
        try:
            db_path = args["db_path"]
            nam_id = args["nam_id"]
            critical_col = args["critical_col"]
            f1525 = args["f1525"]
            folder_save = f"{FOLDER}/{critical_col}_OLD_"
            if f1525: folder_save += "1525"
            else: folder_save += "NORM"
            os.makedirs(folder_save, exist_ok=True)
            filter_old(db_path, nam_id, TARGET, NUM_FIELD, LEVEL, folder_save, critical_col, f1525)
            return ("Success", None, args)
        except Exception as ex:
            return ("Failed", ex, args)
    elif task_name == "TK_new":
        try:
            db_path = args["db_path"]
            nam_id = args["nam_id"]
            critical_col = args["critical_col"]
            folder_save = f"{FOLDER}/{critical_col}_NEW_"
            if db_path.endswith("_1525_temp.db"): folder_save += "1525"
            else: folder_save += "NORM"
            os.makedirs(folder_save, exist_ok=True)
            df = pd.read_excel(DATA_PATH)
            df = df[df["TIME"] < YEAR_MIN + nam_id].reset_index(drop=True)
            df.index += 1
            df.loc[0] = {"TIME": YEAR_MIN + nam_id, "SYMBOL": "_NULL_", "EXCHANGE": "_NULL_"}
            df.sort_index(inplace=True)
            vis = Base(df, INTEREST, VALUEARG_THRESHOLD)
            filter_new(vis, db_path, nam_id, TARGET, RATE, folder_save, critical_col, EVAL_METHOD, EXCLUDE_THRESHOLD)
            return ("Success", None, args)
        except Exception as ex:
            return ("Failed", ex, args)
    else:
        return ("Invalid task_name", None, args)


def run_merge_table(db_path: str, critical_col: str):
    try:
        merge_table(db_path, critical_col)
        return (db_path, "merge_table", "success")
    except Exception as ex:
        return (db_path, "merge_table", f"failed: {ex}")


def run_filter_1525(db_path: str):
    try:
        filter_1525(db_path)
        return (db_path, "filter_1525", "success")
    except Exception as ex:
        return (db_path, "filter_1525", f"failed: {ex}")


def run_filter_unique_profit_value(db_path: str, critical_col: str, target: int = 100_000, filter_1525: bool = False):
    try:
        filter_unique_profit_value(db_path, critical_col, target=target, filter_1525=filter_1525)
        return (db_path, f"filter_unique_profit_value (filter_1525={filter_1525})", "success")
    except Exception as ex:
        return (db_path, f"filter_unique_profit_value (filter_1525={filter_1525})", f"failed: {ex}")


if __name__ == "__main__":
    # Step 1: merge_table
    with Pool(processes=len(list_db_path)) as pool:
        results_merge = pool.starmap(run_merge_table, list_db_path)
    print("\n--- Merge Table Results ---")
    for res in results_merge:
        print(res)

    # Step 2: filter_1525
    list_db_new = [(path[:-3] + "_new.db") for (path, _) in list_db_path]
    with Pool(processes=len(list_db_new)) as pool:
        results_filter_1525 = pool.map(run_filter_1525, list_db_new)
    print("\n--- Filter 1525 Results ---")
    for res in results_filter_1525:
        print(res)

    # Step 3: filter_unique_profit_value
    # Lần 1: filter_1525 = False
    params_filter_unique_false = [
        (path, critical_col, 100_000, False)
        for path, critical_col in zip(list_db_new, [name for _, name in list_db_path])
    ]
    with Pool(processes=len(params_filter_unique_false)) as pool:
        results_filter_unique_false = pool.starmap(run_filter_unique_profit_value, params_filter_unique_false)
    print("\n--- Filter Unique Profit Value (filter_1525=False) Results ---")
    for res in results_filter_unique_false:
        print(res)

    # Lần 2: filter_1525 = True
    params_filter_unique_true = [
        (path, critical_col, 100_000, True)
        for path, critical_col in zip(list_db_new, [name for _, name in list_db_path])
    ]
    with Pool(processes=len(params_filter_unique_true)) as pool:
        results_filter_unique_true = pool.starmap(run_filter_unique_profit_value, params_filter_unique_true)
    print("\n--- Filter Unique Profit Value (filter_1525=True) Results ---")
    for res in results_filter_unique_true:
        print(res)

    # Chạy tổng kết công thức
    list_task = []

    # Build TK_old tasks
    list_db_new = [(path[:-3] + "_new.db", critical_col) for (path, critical_col) in list_db_path]
    print(list_db_new)
    for db_new, critical_col in list_db_new:
        print(db_new, critical_col)

        conn = sqlite3.connect(db_new)
        curs = conn.cursor()
        curs.execute(get_table_names())
        list_table = [t[0] for t in curs.fetchall()]
        conn.close()
        print(list_table)

        for t in list_table:
            args = {"name": "TK_old", "db_path": db_new, "critical_col": critical_col}
            if t.startswith("TT"):
                nam_id = int(t[2:])
                f1525 = True
            else:
                nam_id = int(t[1:])
                f1525 = False
            args.update({
                "nam_id": nam_id,
                "f1525": f1525
            })
            list_task.append(args)

    # Build TK_new tasks (normal)
    list_db_temp = [(path[:-3] + "_temp.db", critical_col) for (path, critical_col) in list_db_path]
    print(list_db_temp)
    for db_temp, critical_col in list_db_temp:
        print(db_temp, critical_col)

        conn = sqlite3.connect(db_temp)
        curs = conn.cursor()
        curs.execute(get_table_names())
        list_table = [t[0] for t in curs.fetchall()]
        conn.close()
        print(list_table)

        for t in list_table:
            args = {"name": "TK_new", "db_path": db_temp, "critical_col": critical_col}
            args["nam_id"] = int(t[1:])
            list_task.append(args)

    # Build TK_new tasks (1525)
    list_db_1525 = [(path[:-3] + "_1525_temp.db", critical_col) for (path, critical_col) in list_db_path]
    print(list_db_1525)
    for db_1525, critical_col in list_db_1525:
        print(db_1525, critical_col)

        conn = sqlite3.connect(db_1525)
        curs = conn.cursor()
        curs.execute(get_table_names())
        list_table = [t[0] for t in curs.fetchall()]
        conn.close()
        print(list_table)

        for t in list_table:
            args = {"name": "TK_new", "db_path": db_1525, "critical_col": critical_col}
            args["nam_id"] = int(t[1:])
            list_task.append(args)

    for task in list_task:
        print(task)

    with Pool(processes=cpu_count()) as p:
        result = p.map(run_task, list_task)

    for r in result:
        print(r)