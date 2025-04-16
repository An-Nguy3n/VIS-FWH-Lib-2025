from PySources.TKCT_new import MixedSingleDoubleTriple, SingleYear_Har_Invest, DoubleYear_Har_Invest, TripleYear_Har_Invest
from PySources.base import Base, pd, np, calculate_formula, calculate_formula_v2, convert_strF_to_arrF


def get_info_invest(data: pd.DataFrame, interest: float, valuearg_threshold: float,
                    eval_method: str, list_formula: list[str]):
    """
    Parameters
    ----------
    data : DataFrame
    interest : float
    valuearg_threshold : float
    eval_method : {'classic', 'root'}, optional
    list_formula : list of string formula
    """
    assert eval_method in ["classic", "root"]
    vis = Base(data, interest, valuearg_threshold)
    temp_1 = np.empty(vis.OPERAND.shape[1], dtype=np.float64)
    list_result = []
    for formula in list_formula:
        ct = convert_strF_to_arrF(formula)
        if eval_method == "classic":
            weight = calculate_formula(ct, vis.OPERAND, temp_1)
        else:
            weight = calculate_formula_v2(ct, vis.OPERAND, temp_1)
        if abs(weight.max() - weight.min()) <= 2e-6:
            list_result.append({
                "Note": "Value có giá trị tuyệt đối quá nhỏ"
            })
            continue
        Val1, Har1, Val2, Har2, Val3, Har3 = MixedSingleDoubleTriple(
            weight, vis.INDEX, vis.PROFIT, vis.SYMBOL, vis.INTEREST, vis.BOOL_ARG
        )
        list_invest_1 = SingleYear_Har_Invest(weight, vis.BOOL_ARG, Val1)
        list_invest_2 = DoubleYear_Har_Invest(weight, vis.INDEX, vis.SYMBOL, vis.BOOL_ARG, Val2)
        list_invest_3 = TripleYear_Har_Invest(weight, vis.INDEX, vis.SYMBOL, vis.BOOL_ARG, Val3)

        size = vis.INDEX.shape[0] - 1
        result = {
            "Ngn1": {
                "ValNgn1": Val1,
                "HarNgn1": Har1,
                "Invest": {}
            },
            "Ngn2": {
                "ValNgn2": Val2,
                "HarNgn2": Har2,
                "Invest": {}
            },
            "Ngn3": {
                "ValNgn3": Val3,
                "HarNgn3": Har3,
                "Invest": {}
            },
        }
        year = int(vis.data.iloc[-1]["TIME"])

        for i in range(size-1, -1, -1):
            start, end = vis.INDEX[i], vis.INDEX[i+1]
            invest = np.where(list_invest_1[start:end])[0]
            w_ = weight[invest]
            arg = np.argsort(w_, kind="stable")[::-1]
            invest = invest[arg] + start
            w_ = w_[arg]
            result["Ngn1"]["Invest"][year] = {}
            for j in range(len(invest)):
                x = invest[j]
                result["Ngn1"]["Invest"][year][vis.symbol_name[vis.SYMBOL[x]]] = {
                    "Value": float(w_[j]),
                    "Profit": float(vis.PROFIT[x])
                }

            if i <= size - 2:
                invest = np.where(list_invest_2[start:end])[0]
                w_ = weight[invest]
                arg = np.argsort(w_, kind="stable")[::-1]
                invest = invest[arg] + start
                w_ = w_[arg]
                result["Ngn2"]["Invest"][year] = {}
                for j in range(len(invest)):
                    x = invest[j]
                    result["Ngn2"]["Invest"][year][vis.symbol_name[vis.SYMBOL[x]]] = {
                        "Value": float(w_[j]),
                        "Profit": float(vis.PROFIT[x])
                    }

            if i <= size - 3:
                invest = np.where(list_invest_3[start:end])[0]
                w_ = weight[invest]
                arg = np.argsort(w_, kind="stable")[::-1]
                invest = invest[arg] + start
                w_ = w_[arg]
                result["Ngn3"]["Invest"][year] = {}
                for j in range(len(invest)):
                    x = invest[j]
                    result["Ngn3"]["Invest"][year][vis.symbol_name[vis.SYMBOL[x]]] = {
                        "Value": float(w_[j]),
                        "Profit": float(vis.PROFIT[x])
                    }

            year += 1

        list_result.append(result)

    return list_result
