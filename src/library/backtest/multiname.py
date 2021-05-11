import pandas as pd

from src.library.backtest.singlename import ticker_bo_ts
from src.library.helpers.dates import get_dates
from src.library.osmv.Osmv import Osmv
from src.library.params import param
from src.library.params import timeserie as ts_param

(list_of_dates, list_obj_dates) = get_dates()

greek_ts_cols = ts_param.GREEK_TS_COLS
cost_cols = ts_param.COST_COLS
keep_cols = ts_param.KEEP_COLS

osmv = Osmv(param.IS_LOCAL, param.BUCKET_NAME)
(dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.select_env(param.ENV_USED)


def common_dates(dict_ticker_ts):
    list_dates = []
    for ticker, dict_df in dict_ticker_ts.items():
        df = list(dict_df.values())[0]
        list_dates.append(list(df.trade_date))
    if len(list_dates) > 0:
        return sorted(list(set(list_dates[0]).intersection(*list_dates)))
    else:
        return list_dates[0]


def align_dates(dict_ticker_ts, columns_to_align):
    dates = common_dates(dict_ticker_ts)
    for ticker, dict_df in dict_ticker_ts.items():
        for strike, df in dict_df.items():
            aligned_df = align_df(df, columns_to_align, dates)
            dict_df[strike] = aligned_df

    return dict_ticker_ts, dates


def align_df(df, columns_to_align, com_dates):
    dates_dict = {}
    idx_common_dates = {}

    for d in com_dates:
        idx_common_dates[d] = list(df.trade_date).index(d)

    dates_dict[com_dates[0]] = [df.trade_date.iloc[0]]
    for i, d in enumerate(com_dates[1:]):
        dates_dict[d] = list(df.trade_date.iloc[(idx_common_dates[com_dates[i]] + 1):(idx_common_dates[d] + 1)].values)

    output_df = df.copy()
    output_df = output_df[output_df.trade_date.isin(com_dates)]

    for col in columns_to_align:
        output_df[col] = output_df.trade_date.apply(lambda d: df[df.trade_date.isin(dates_dict[d])][col].sum())
    return output_df


def equi_weighted_ts_dict(greek, strikes, dict_ticker_ts):
    #     longs
    invert = 1
    if greek == "theta":
        invert = -1
    num_stocks = 0

    all_cols = greek_ts_cols + cost_cols

    (dict_ticker_ts, dates) = align_dates(dict_ticker_ts, ["cdh_pnl"])

    output_dict = {}

    for K in strikes:

        data = {}
        data["trade_date"] = dates

        dict_tickers = {}
        for ticker, strikes_dict in dict_ticker_ts.items():
            df = strikes_dict[K]
            # check timing of units calc, using greek D-1
            no_zeros = (df[greek] != 0).all()
            if no_zeros:
                df["units"] = invert / df[greek]
                data["units " + ticker] = df["units"]
                for col in all_cols:
                    df[col + "_weighted"] = df["units"] * df[col]

                df["cdh_pnl_weighted"] = df["units"].shift(periods=1) * df["cdh_pnl"]

                dict_tickers[ticker] = df
                num_stocks += 1

        for col in (all_cols + ["cdh_pnl"]):
            data[col] = sum([dict_tickers[t][col + "_weighted"] for t in dict_tickers.keys()]) / num_stocks

        sum_df = pd.DataFrame(data)
        output_dict[K] = sum_df

    units_columns = [c for c in output_dict[strikes[0]].columns if c[:5] == "units"]
    return (output_dict, units_columns)


def greek_neutral_ts_dict(greek, strikes, start, end, maturity_string, tickers_long, tickers_short, threshold,
                          export_dfs=False):
    exports = {}
    exceptions = {}

    #     longs
    dict_ticker_ts = {}
    for t in tickers_long:
        try:
            (strikes_dict, ts_exports) = ticker_bo_ts(t, strikes, maturity_string, start, end, threshold,
                                                      export_dfs=True)

            if export_dfs:
                exports.update(ts_exports)
                for k in strikes:
                    sheet_key = t + "_" + maturity_string + "_k=" + str(k) + "_" + start
                    meta_dict = {"ticker": t,
                                 "rolling maturity": maturity_string,
                                 "strike": k,
                                 "start date": start,
                                 "end date": end,
                                 "threshold": threshold}
                    exports[sheet_key] = {"df": strikes_dict[k], "meta": meta_dict}

            dict_ticker_ts[t] = strikes_dict

        except Exception as e:
            exceptions[start + "-" + t] = e

    (long_dict, long_units_columns) = equi_weighted_ts_dict(greek, strikes, dict_ticker_ts)

    #     shorts
    dict_ticker_ts = {}
    for t in tickers_short:
        try:
            (strikes_dict, ts_exports) = ticker_bo_ts(t, strikes, maturity_string, start, end, threshold,
                                                      export_dfs=True)

            if export_dfs:
                exports.update(ts_exports)
                for k in strikes:
                    sheet_key = t + "_" + maturity_string + "_k=" + str(k) + "_" + start
                    meta_dict = {"ticker": t,
                                 "rolling maturity": maturity_string,
                                 "strike": k,
                                 "start date": start,
                                 "end date": end,
                                 "threshold": threshold}
                    exports[sheet_key] = {"df": strikes_dict[k], "meta": meta_dict}

            dict_ticker_ts[t] = strikes_dict
        except Exception as e:
            exceptions[start + "-" + t] = e

    (short_dict, short_units_columns) = equi_weighted_ts_dict(greek, strikes, dict_ticker_ts)

    ls_dict = {"long": long_dict, "short": short_dict}

    (ls_dict, common_dates) = align_dates(ls_dict, ["cdh_pnl"])

    output_dict = {}
    ls_data = {}
    ls_data["trade_date"] = common_dates

    for K in strikes:

        for col in keep_cols:
            ls_data[col] = ls_dict["long"][K][col]

        for col in cost_cols:
            ls_data[col] = ls_dict["long"][K][col]

        ls_df = pd.DataFrame(ls_data)

        ls_df["gross_pnl"] = ls_df["cdh_pnl"].cumsum()
        ls_df["net_pnl"] = ls_df["cdh_pnl"].cumsum() - ls_df["trans_cost"].cumsum()
        ls_df.loc[ls_df["trade_date"] == start, "gross_pnl"] = 0
        ls_df.loc[ls_df["trade_date"] == start, "net_pnl"] = 0

        units_ls_df = pd.concat(
            (ls_df, ls_dict["long"][K][long_units_columns], ls_dict["short"][K][short_units_columns]), axis=1)
        output_dict[K] = units_ls_df

    return (output_dict, exports, exceptions)


def greek_neutral_ts(greek, strike, start, end, maturity_string, tickers_long, tickers_short, threshold,
                     export_dfs=False):
    (output_dict, exports, exceptions) = greek_neutral_ts_dict(greek, [strike], start, end, maturity_string,
                                                               tickers_long, tickers_short, threshold, export_dfs)
    return (output_dict[strike], exports, exceptions)
