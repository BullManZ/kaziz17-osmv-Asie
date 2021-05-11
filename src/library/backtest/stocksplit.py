import pandas as pd

import src.library.params.param as param
from src.library.dynamo.Table import Table
from src.library.osmv.Osmv import Osmv
from src.library.params import columns

osmv = Osmv(param.IS_LOCAL, param.BUCKET_NAME)
(dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.select_env(param.ENV_USED)
splits_table = Table(dbr.Table(db_dict["splits_table"]))


def stock_split_ref(df, stock_split, ref, prev_df):

    strike = float(ref[(ref.rfind('-')+1):len(ref)])
    available_strikes = set(df.strike)
    adj_strike = find_closest_strike(strike, stock_split, available_strikes)
    df_k = df[df.strike == adj_strike]
    prev_df_k = prev_df[prev_df.strike == strike]
    df_k = calc_pnl(prev_df_k, df_k, stock_split)
    return df_k, adj_strike


def calc_pnl(prev_df_k, df_k, stock_split):
    col_moves = ["spot", "i_rate", "c_mid", "p_mid", "yte", "c_iv_mid", "p_iv_mid", "osmv_vol"]
    ss_moves = ["spot", "c_mid", "p_mid"]

    df_prev = pd.DataFrame(prev_df_k).set_index("expi")
    df_prev.columns = [str(col) + '_prev' for col in df_prev.columns]

    df = pd.DataFrame(df_k).set_index("expi")
    dt = pd.concat((df, df_prev), axis=1)

    for col in col_moves:
        dt[col + "_move"] = dt[col] - dt[col + "_prev"]

    for col in ss_moves:
        dt[col + "_move"] = dt[col] - dt[col + "_prev"]/stock_split

    # pnls
    dt["s_mid_move"] = dt.c_mid_move - dt.p_mid_move
    dt["c_delta_pnl"] = dt.delta_prev * dt.s_mid_move
    dt["p_delta_pnl"] = (dt.delta_prev - 1) * dt.s_mid_move
    dt["theta_pnl"] = dt.theta_prev * dt.yte_move
    dt["gamma_pnl"] = dt.gamma_prev * 0.5 * dt.spot_move ** 2
    dt["rho_pnl"] = dt.rho_prev * dt.i_rate_move

    # pnl explains
    dt["c_pnl_explain"] = dt.c_delta_pnl + dt.gamma_pnl + dt.rho_pnl - dt.theta_pnl
    dt["p_pnl_explain"] = dt.p_delta_pnl + dt.gamma_pnl - dt.rho_pnl - dt.theta_pnl

    dt["cdh_pnl"] = dt.c_mid_move - dt.c_delta_pnl
    dt["pdh_pnl"] = dt.p_mid_move - dt.p_delta_pnl

    dt["c_vol_pnl_mid"] = dt.c_mid_move - dt.c_pnl_explain
    dt["p_vol_pnl_mid"] = dt.p_mid_move - dt.p_pnl_explain

    dt["otm_iv_mid_move"] = dt["c_iv_mid_move"] * (dt.delta < 0.5) + dt["c_iv_mid_move"] * (dt.delta >= 0.5)

    dt["vega_prev"] = dt["vega_prev"].apply(lambda x: 0.000001 if x == 0 else x)

    dt["c_imp_vol_move"] = dt["c_vol_pnl_mid"] / dt["vega_prev"] / 100
    dt["p_imp_vol_move"] = dt["p_vol_pnl_mid"] / dt["vega_prev"] / 100

    dt = dt.reset_index().rename(columns={'index': 'expi'})

    return dt[columns.all_cols + columns.pnl_cols]


def disappeared_ref(date, ref):
    data_dict = {c: 0 for c in (columns.all_cols + columns.pnl_cols)}
    data_dict["trade_date"] = date
    data_dict["ref"] = ref
    return pd.DataFrame(index=[0], data=data_dict)


def find_closest_strike(strike, stock_split, available_strikes):
    theo_strike = strike / stock_split
    return min(available_strikes, key=lambda x: abs(x - theo_strike))


def lookup_ss(ticker, date):
    item = splits_table.get_item("ticker", ticker, True, "trade_date", date)
    if not item:
        return False, 0
    else:
        return True, float(item["value"])