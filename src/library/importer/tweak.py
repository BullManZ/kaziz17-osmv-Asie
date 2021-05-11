import datetime

import pandas as pd

import src.library.params.columns as columns
import src.library.params.import_param as import_param

volume_decay = 0.9
oi_decay = 0.8
volume_alpha = 0.2
oi_alpha = 0.1


def tweak(df):
    # Converting the structure of the dataframe

    df_call = df[df['call_put'] == 'C']
    df_call.reset_index(inplace=True)

    df_put = df[df['call_put'] == 'P']
    df_put.reset_index(inplace=True)

    df_call.rename(columns={"bid": "c_bid", "ask": "c_ask", "volume": "c_volume", "open_interest": "c_open_interest"},
                   inplace=True)
    call_dict = {}
    call_dict = {"bid": "c_bid", "ask": "c_ask", "volume": "c_volume", "open_interest": "c_open_interest"}

    # ##Rename df_put c_bid ...
    put_dict = {"bid": "p_bid", "ask": "p_ask", "volume": "p_volume", "open_interest": "p_open_interest"}

    df_put.rename(columns={"bid": "p_bid", "ask": "p_ask", "volume": "p_volume", "open_interest": "p_open_interest"},
                  inplace=True)

    # df_put.rename(columns=put_dict, inplace=True)
    df_put.tail()

    temp = []
    for col in put_dict.values():
        temp = df_put[col]
        df_call = df_call.join(temp)
    df_joined = df_call

#    df_joined = pd.concat([df_call,df_put], axis = 1)

    df_joined["c_mid"] = 0.5 * (df_joined["c_ask"] + df_joined["c_bid"])
    df_joined["p_mid"] = 0.5 * (df_joined["p_ask"] + df_joined["p_bid"])

    list_DROP = ['call_put']
    df_joined.drop(columns=list_DROP, inplace=True)


    df_joined['date'] = pd.to_datetime(df_joined['date'], yearfirst=True)

    df_joined['expirDate'] = pd.to_datetime(df_joined['expirDate'], yearfirst=True)

    df_joined['day_of_week'] = df_joined['expirDate'].dt.day_name()


    df.groupby(['expirDate']).sum(['open_interest'])

    date_diff = []
    date_diff = (df_joined['expirDate'] - df_joined['date'])

    df_joined["yte"] = date_diff.dt.days / 365

    df_joined["total_oi"] = df_joined["c_open_interest"] + df_joined["p_open_interest"]

    df_joined["cAskIv"] = df_joined["iv"] + 0.1 * (df_joined["c_ask"] - df_joined["c_bid"]) / df_joined["vega"]
    df_joined["cBidIv"] = df_joined["iv"] + 0.1 * (df_joined["c_bid"] - df_joined["c_ask"]) / df_joined["vega"]
    df_joined["cMidIv"] = 0.5 * (df_joined["cAskIv"] + df_joined["cBidIv"])

    df_joined["pAskIv"] = df_joined["iv"] + 0.1 * (df_joined["p_ask"] - df_joined["p_bid"]) / df_joined["vega"]
    df_joined["pBidIv"] = df_joined["iv"] + 0.1 * (df_joined["p_bid"] - df_joined["p_ask"]) / df_joined["vega"]
    df_joined["pMidIv"] = 0.5 * (df_joined["pAskIv"] + df_joined["pBidIv"])

    df_joined = df_joined.rename(
        columns={'date': 'trade_date', 'symbol': 'ticker', 'stock_price_close': 'spot', 'expirDate': 'expi',
                 'c_volume': 'c_volu', 'p_volume': 'p_volu', 'volume': 'total_volu', 'c_open_interest': 'c_oi',
                 'p_open_interest': 'p_oi', 'total_open_interest': 'total_oi', 'cBidPx': 'c_bid', 'cAskPx': 'c_ask',
                 'pBidPx': 'p_bid', 'pAskPx': 'p_ask', 'cBidIv': 'c_iv_bid', 'cMidIv': 'c_iv_mid', 'cAskIv': 'c_iv_ask',
                 'pBidIv': 'p_iv_bid', 'pMidIv': 'p_iv_mid', 'pAskIv': 'p_iv_ask', }
        )

    df_joined = df_joined.drop(import_param.DROP_LIST, axis=1)

    df_joined = drop_expis(df_joined)

    # dff = filter_c_value(dff)

    df_joined["c_mid"] = 0.5 * (df_joined.c_ask + df_joined.c_bid)
    df_joined["p_mid"] = 0.5 * (df_joined.p_ask + df_joined.p_bid)
    df_joined["ref"] = df_joined["ticker"].astype(str) + "-" + df_joined["expi"].astype(str) + "-" + df_joined[
        "strike"].astype(str)
    df_joined["total_volu"] = df_joined["c_volu"] + df_joined["p_volu"]
    df_joined["strike_%"] = df_joined["strike"] / df_joined["spot"]
    df_joined["s_ask"] = df_joined["c_ask"] - df_joined["p_ask"]
    df_joined["s_bid"] = df_joined["c_bid"] - df_joined["p_bid"]
    # dff["c_value"] = dff.c_value - dff.p_value
    df_joined["c_ba"] = df_joined["c_iv_ask"] - df_joined["c_iv_bid"]
    df_joined["p_ba"] = df_joined["p_iv_ask"] - df_joined["p_iv_bid"]
    df_joined["min_ba"] = df_joined[["c_ba", "p_ba"]].min(axis=1)

    return df_joined[columns.all_cols]


def drop_expis(dff):
    #    dff["expi_dt"] = dff["expi"].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))

    dff["expi_dt"] = dff["expi"]
    dff["expi_weekday"] = dff["expi_dt"].apply(lambda x: x.weekday())
    dff = dff[dff.expi_weekday.isin((3,4))]
    dff["is_sat"] = dff.expi_weekday == 4

    dff_sat = dff[dff.is_sat]
    dff_sat["expi_dt"] = dff_sat["expi_dt"] + datetime.timedelta(days=-1)
    dff_fri = dff[~dff.is_sat]

    dff = pd.concat((dff_fri, dff_sat), axis=0)
    dff["expi"] = dff.expi_dt.apply(lambda x: x.strftime('%Y-%m-%d'))
    dff["is_expi_3rd_friday"] = dff["expi_dt"].apply(lambda x: 15 <= x.day <= 21)

    dff = dff[dff.is_expi_3rd_friday]
    dff = dff.drop(["expi_weekday", "is_expi_3rd_friday", "expi_dt", "is_sat"], axis=1)
    return dff


# def filter_c_value(df):
#     return df[df.c_value != 99999.99]


def calc_pnl(day_df, prev_day_df):
    col_moves = ["spot", "c_mid", "p_mid", "yte", "c_iv_mid", "p_iv_mid"]

    df_prev = pd.DataFrame(prev_day_df).set_index("ref")
    df_prev.columns = [str(col) + '_prev' for col in df_prev.columns]
    #print(df_prev.head())

    df = pd.DataFrame(day_df).set_index("ref")
    dt = pd.concat((df, df_prev), axis=1)

    for col in col_moves:
        dt[col + "_move"] = dt[col] - dt[col + "_prev"]
    # pnls
    dt["s_mid_move"] = dt.c_mid_move - dt.p_mid_move
    dt["c_delta_pnl"] = dt.delta_prev * dt.s_mid_move
    dt["p_delta_pnl"] = (dt.delta_prev - 1) * dt.s_mid_move
    dt["theta_pnl"] = dt.theta_prev * dt.yte_move
    dt["gamma_pnl"] = dt.gamma_prev * 0.5 * dt.spot_move ** 2
    dt["rho_pnl"] = 0 #dt.rho_prev * dt.i_rate_move

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

    dt = dt[dt.index.isin(set(day_df.ref))]
    dt = dt.reset_index().rename(columns={'index': 'ref'})
    return dt
