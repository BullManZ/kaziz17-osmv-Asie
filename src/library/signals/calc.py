import pandas as pd

from src.library.params import rviv as C_RVIV
from src.library.signals.rviv import query_rviv

mov_window = C_RVIV.MOV_WINDOW


def proba_threshold(x, threshold):
    num = 0
    for i in x:
        num += 1 if i > threshold else 0
    return num / len(x)


pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]


def calc_ratio(data, ref_data):

    #keep dates in common
    ref_data_dates = list(ref_data.index)
    data_dates = list(data.index)
    common_dates = list(set(ref_data_dates) & set(data_dates))

    data = data[data.index.isin(common_dates)].sort_index(ascending=True)
    ref_data = ref_data[ref_data.index.isin(common_dates)].sort_index(ascending=True)

    # keep columns in common
    ref_data_cols = ref_data.columns
    data_cols = data.columns
    keep_cols =  (set(ref_data_cols) & set(data_cols)) - {"trade_date","spot","ticker"}
    return data[keep_cols] / ref_data[keep_cols]


def calc_signals(ticker, ref_data, selected_signals):

    data = query_rviv(ticker)
    ratio = calc_ratio(data, ref_data)

    ratio.rename(columns={v: ("ratio_" + v) for v in ratio.columns}, inplace=True)
    all_data = pd.concat((data, ratio), axis=1)

    rolling_cols = [c for c in all_data.columns if c[-2:] in selected_signals["maturity_suffix"]]
    pct_cols = [c for c in rolling_cols if c[:-2] in selected_signals["pct_prefix"]]
    median_cols = [c for c in rolling_cols if c[:-2] in selected_signals["median_prefix"]]
    proba_1_cols = [c for c in rolling_cols if c[:-2] in selected_signals["proba_1_prefix"]]
    rolling = all_data[rolling_cols].rolling(mov_window, min_periods=1)
    pct = rolling[pct_cols].apply(pctrank).rename(columns={v: ("perc%_" + v) for v in all_data.columns})
    median = rolling[median_cols].median().rename(columns={v: ("median_" + v) for v in all_data.columns})
    proba_1 = rolling[proba_1_cols].apply(lambda x: proba_threshold(x, 1)).rename(
        columns={v: ("proba_1_" + v) for v in all_data.columns})

    return pd.concat((data[["trade_date","ticker","spot"]], pct, median, proba_1), axis=1)


def select_signals(maturity_suffix, pct_prefix, median_prefix, proba_1_prefix):
    sigs = []
    for m in maturity_suffix:
        for p in pct_prefix:
            sigs.append("perc%_" + p + m)
        for p in median_prefix:
            sigs.append("median_" + p + m)
        for p in proba_1_prefix:
            sigs.append("proba_1_" + p + m)

    output = {}
    output["maturity_suffix"] = maturity_suffix
    output["pct_prefix"] = pct_prefix
    output["median_prefix"] = median_prefix
    output["proba_1_prefix"] = proba_1_prefix
    output["all"] = sigs

    return output
