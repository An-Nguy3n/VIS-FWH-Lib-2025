import pandas as pd
import numpy as np
import numba as nb


class Base:
    def __init__(
            self,
            data: pd.DataFrame,
            interest: float,
            valuearg_threshold: float
    ) -> None:
        '''
        Các cột bắt buộc phải có trong data: TIME, PROFIT, SYMBOL, VALUEARG.
        Các cột còn lại mà có kiểu dữ liệu là int64 hoặc float64 được coi là biến để sinh công thức.
        '''
        data = data.reset_index(drop=True)
        data.fillna(0.0, inplace=True)

        # Check cac cot bat buoc
        drop_cols = ["TIME", "PROFIT", "SYMBOL", "VALUEARG"]
        for col in drop_cols:
            if col not in data.columns:
                raise Exception(f"Thieu cot {col}")

        # Check dtype cua TIME, PROFIT va VALUEARG
        if data["TIME"].dtype != "int64":
            raise Exception("TIME's dtype must be int64")
        if data["PROFIT"].dtype != "float64":
            raise Exception("PROFIT's dtype must be float64")
        if data["VALUEARG"].dtype not in ["int64", "float64"]:
            raise Exception("VALUEARG's dtype must be int64 or float64")

        # Check thu tu cot TIME va min PROFIT, min VALUEARG
        if data["TIME"].diff().max() > 0:
            raise Exception("Cot TIME phai giam dan")
        if data["PROFIT"].min() < 0.0:
            raise Exception("PROFIT < 0.0")
        if data["VALUEARG"].min() < 0.0:
            raise Exception("VALUEARG < 0.0")

        # INDEX
        index = []
        temp = data["TIME"].unique()
        for i in range(temp.max(), temp.min()-1, -1):
            if i not in temp: raise Exception(f"Thieu chu ky {i}")
            index.append(data[data["TIME"]==i].index[0])
        self.INDEX = np.array(index + [data.shape[0]])

        # Check SYMBOL co unique trong tung chu ky hay khong
        for i in range(self.INDEX.shape[0] - 1):
            start, end = self.INDEX[i], self.INDEX[i+1]
            if len(data.loc[start:end-1, "SYMBOL"].unique()) != (end - start):
                raise Exception("SYMBOL khong unique o tung chu ky")

        # Loai cac cot co kieu du lieu khong phai int64 va float64
        for col in data.columns:
            if col not in drop_cols and data[col].dtype not in ["int64", "float64"]:
                drop_cols.append(col)

        self.drop_cols = drop_cols
        print("Cac cot khong duoc coi la bien:", self.drop_cols)

        # Attrs
        self.data = data
        self.INTEREST = interest
        self.PROFIT = np.array(data["PROFIT"], float)
        self.PROFIT[self.PROFIT < 5e-324] = 5e-324
        self.VALUEARG = np.array(data["VALUEARG"], float)
        self.BOOL_ARG = self.VALUEARG >= valuearg_threshold

        symbol_name = data["SYMBOL"].unique()
        self.symbol_name = {symbol_name[i]:i for i in range(len(symbol_name))}
        self.SYMBOL = np.array([self.symbol_name[s] for s in data["SYMBOL"]])
        self.symbol_name = {v:k for k,v in self.symbol_name.items()}

        operand_data = data.drop(columns=drop_cols)
        operand_name = operand_data.columns
        self.operand_name = {i:operand_name[i] for i in range(len(operand_name))}
        self.OPERAND = np.transpose(np.array(operand_data, float))


@nb.njit
def calculate_formula(formula, operand):
    temp_0 = np.zeros(operand.shape[1])
    temp_1 = np.zeros(operand.shape[1])
    temp_op = -1
    num_operand = operand.shape[0]
    for i in range(1, formula.shape[0], 2):
        if formula[i] >= num_operand: raise

        if formula[i-1] < 2:
            temp_op = formula[i-1]
            temp_1 = operand[formula[i]].copy()
        else:
            if formula[i-1] == 2:
                temp_1 *= operand[formula[i]]
            else:
                temp_1 /= operand[formula[i]]

        if i+1 == formula.shape[0] or formula[i+1] < 2:
            if temp_op == 0:
                temp_0 += temp_1
            else:
                temp_0 -= temp_1

    temp_0[np.isnan(temp_0)] = -1.7976931348623157e+308
    temp_0[np.isinf(temp_0)] = -1.7976931348623157e+308
    return temp_0


@nb.njit
def decode_formula(f, len_):
    rs = np.zeros(len(f)*2, dtype=np.int64)
    rs[0::2] = f // len_
    rs[1::2] = f % len_
    return rs


@nb.njit
def encode_formula(f, len_):
    return f[0::2] * len_ + f[1::2]


__STRING_OPERATOR = "+-*/"

def convert_arrF_to_strF(arrF):
    strF = ""
    for i in range(len(arrF)):
        if i % 2 == 1:
            strF += str(arrF[i])
        else:
            strF += __STRING_OPERATOR[arrF[i]]

    return strF

def convert_strF_to_arrF(strF):
    f_len = sum(strF.count(c) for c in __STRING_OPERATOR) * 2
    str_len = len(strF)
    arrF = np.zeros(f_len, dtype=int)

    idx = 0
    for i in range(f_len):
        if i % 2 == 1:
            t_ = 0
            while True:
                t_ = 10*t_ + int(strF[idx])
                idx += 1
                if idx == str_len or strF[idx] in __STRING_OPERATOR:
                    break
            arrF[i] = t_
        else:
            arrF[i] = __STRING_OPERATOR.index(strF[idx])
            idx += 1

    return arrF
