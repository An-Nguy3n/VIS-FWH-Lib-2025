from base import Base
import numpy as np
import pandas as pd
import sys
import os


def to_bin_file(base, filename, dtype, folder_save):
    array = getattr(base, filename)
    with open(f"{folder_save}/InputData/{filename}.bin", "wb") as f:
        f.write(np.array(array.shape, np.int32).tobytes())
        f.write(np.array(array, dtype).tobytes())


def extract_data():
    data_path = sys.argv[1]
    interest = float(sys.argv[2])
    valuearg_threshold = float(sys.argv[3])
    folder_save = sys.argv[4]

    os.makedirs(f"{folder_save}/InputData/", exist_ok=True)
    for file in os.listdir(f"{folder_save}/InputData/"):
        if file != "checkpoint.bin":
            os.remove(f"{folder_save}/InputData/{file}")

    base = Base(pd.read_excel(data_path), interest, valuearg_threshold)
    to_bin_file(base, "INDEX", np.int32, folder_save)
    to_bin_file(base, "PROFIT", np.float64, folder_save)
    to_bin_file(base, "SYMBOL", np.int32, folder_save)
    to_bin_file(base, "BOOL_ARG", np.int32, folder_save)
    to_bin_file(base, "OPERAND", np.float64, folder_save)


if __name__ == "__main__":
    extract_data()
