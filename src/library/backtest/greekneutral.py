import pandas as pd

from src.library.backtest.process import derive_table_name
from src.library.dynamo.Table import Table
from src.library.helpers.dates import get_dates
from src.library.helpers.general import bring_columns_first
from src.library.osmv.Osmv import Osmv
from src.library.params import param
from src.library.params import timeserie as ts_param


#(list_of_dates, list_obj_dates) = get_dates()
from src.library.params.param import BUCKET_NAME

list_obj_dates = []
list_of_dates = []
for key in BUCKET_NAME.list():
    list_of_dates.append((key.name.encode('utf-8')[-8:], "%Y%m%d").date())
    list_obj_dates.append(key.name.encode('utf-8')[-8:])

greek_ts_cols = ts_param.GREEK_TS_COLS
cost_cols = ts_param.COST_COLS
keep_cols = ts_param.KEEP_COLS

osmv = Osmv(param.IS_LOCAL, param.BUCKET_NAME)
(dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.select_env(param.ENV_USED)


# def common_df_dates(dict_ticker_df):
#     list_dates = []
#     for ticker, df in dict_ticker_df.items():
#         list_dates.append(list(df.trade_date))
#     if len(list_dates) > 0:
#         return sorted(list(set(list_dates[0]).intersection(*list_dates)))
#     else:
#         return list_dates[0]
#
#
# def align_df_dates(dict_ticker_df, columns_to_align):
#     dates = common_df_dates(dict_ticker_df)
#     for ticker, df in dict_ticker_df.items():
#         dict_ticker_df[ticker] = align_df(df, columns_to_align, dates)
#
#     return dict_ticker_df, dates
#
#
# def align_df(df, columns_to_align, com_dates):
#     dates_dict = {}
#     idx_common_dates = {}
#
#     for d in com_dates:
#         idx_common_dates[d] = list(df.trade_date).index(d)
#
#     dates_dict[com_dates[0]] = [df.trade_date.iloc[0]]
#     for i, d in enumerate(com_dates[1:]):
#         dates_dict[d] = list(df.trade_date.iloc[(idx_common_dates[com_dates[i]] + 1):(idx_common_dates[d] + 1)].values)
#
#     output_df = df.copy()
#     output_df = output_df[output_df.trade_date.isin(com_dates)]
#
#     for col in columns_to_align:
#         output_df[col] = output_df.trade_date.apply(lambda d: df[df.trade_date.isin(dates_dict[d])][col].sum())
#     return output_df


def load_ticker_df(table_name, ticker, start, end):
    items = Table(dbr.Table(table_name)).query_range_between("ticker", ticker, "trade_date", start, end)
    dates = [i["trade_date"] for i in items]
    if start in dates:
        return pd.DataFrame(items)
    else:
        return pd.DataFrame([])


def greek_neutral_ts(greek, strike, start, end, maturity_string, tickers_long, tickers_short, export_dfs=False):

    table_name = derive_table_name(strike, maturity_string)
    ts_dates = [d for d in list_of_dates if (start <= d <= end)]

    #     longs
    (long_df, long_start_risk_dict, long_end_risk_dict, long_exports) = greek_weighted_ts(greek, tickers_long, table_name, start, end, export_dfs)

    longs = list(long_start_risk_dict.keys())
    #     shorts
    (short_df, short_start_risk_dict, short_end_risk_dict, short_exports) = greek_weighted_ts(greek, tickers_short, table_name, start,
                                                                               end, export_dfs)
    shorts = list(short_start_risk_dict.keys())

    ls_df = pd.DataFrame(index=ts_dates)

    start_risk_dict = net_risk(long_start_risk_dict, short_start_risk_dict)
    end_risk_dict = net_risk(long_end_risk_dict, short_end_risk_dict)

    for col in keep_cols:
        ls_df[col] = long_df[col] - short_df[col]

    for col in cost_cols:
        ls_df[col] = long_df[col] + short_df[col]

    ls_df = ls_df.groupby(ls_df.columns, axis=1).sum()
    long_exports.update(short_exports)

    return ls_df, start_risk_dict, end_risk_dict, longs, shorts, long_exports


def greek_weighted_ts(greek, stocks, table_name, start, end, export_dfs=False):
    dict_ticker_df = {}
    exports = {}
    ts_dates = [d for d in list_of_dates if (start <= d <= end)]

    for t in stocks:
        df = load_ticker_df(table_name, t, start, end)
        if len(df) > 0:
            dict_ticker_df[t] = df
            export_df = bring_columns_first(df, ["trade_date","ticker","expi","spot"])
            if export_dfs:
                    sheet_key = t + "_" + table_name +"_" + start
                    meta_dict = {"ticker": t,
                                 "table name": table_name,
                                 "start date": start,
                                 "end date": end}
                    exports[sheet_key] = {"df": export_df, "meta": meta_dict}


    invert = 1
    if greek == "theta":
        invert = -1
    num_stocks = 0

    all_cols = greek_ts_cols + cost_cols

    start_risk_dict = {}
    end_risk_dict = {}
    kept_tickers = []
    data = {}
    units_series = {}


    for ticker, df in dict_ticker_df.items():
        df.set_index("trade_date", inplace=True)
        # check timing of units calc, using greek D-1
        no_zeros = (df[greek] != 0).all()
        if no_zeros:
            # calc units
            df["units"] = invert / df[greek]
            units_series[ticker] = df["units"]
            units_series[ticker] = add_missing_dates_serie(units_series[ticker], ts_dates)

            # weighted columns
            for col in all_cols:
                df[col + "_weighted"] = df["units"] * df[col]

            # greek weighted pnl
            df["cdh_pnl_weighted"] = df["units"].shift(periods=1) * df["cdh_pnl"]

            df = add_missing_dates_df(df, ts_dates)
            dict_ticker_df[ticker] = df

            num_stocks += 1
            kept_tickers.append(ticker)
            start_risk_dict[ticker] = df.iloc[0][["vega", "min_ba", "units"]]
            end_risk_dict[ticker] = df.iloc[-1][["vega", "min_ba", "units"]]

    if num_stocks > 0:
        for col in (all_cols + ["cdh_pnl"]):
            data[col] = sum([dict_ticker_df[t][col + "_weighted"] for t in kept_tickers]) / num_stocks

        for ticker in kept_tickers:
            start_risk_dict[ticker]["units"] /= num_stocks
            end_risk_dict[ticker]["units"] /= num_stocks
            data["units_" + ticker] = units_series[ticker]

        return pd.DataFrame(index=ts_dates, data=data), start_risk_dict, end_risk_dict, exports
    else:
        return empty_df(ts_dates, all_cols + ["cdh_pnl"]), start_risk_dict, end_risk_dict, exports


def add_missing_dates_df(df, ts_dates):
    add_dates = set(ts_dates) - set(df.index)
    added_dates_df = pd.DataFrame(index=add_dates, columns=df.columns)
    output = pd.concat((df, added_dates_df), axis=0).fillna(0)
    output = output.sort_index(ascending=True)
    return output


def add_missing_dates_serie(df, ts_dates):
    add_dates = set(ts_dates) - set(df.index)
    added_dates_df = pd.Series(index=add_dates)
    output = pd.concat((df, added_dates_df), axis=0).fillna(0)
    output = output.sort_index(ascending=True)
    return output


def empty_df(ts_dates, cols):
    added_dates_df = pd.DataFrame(index=ts_dates, columns=cols)
    output = added_dates_df.fillna(0)
    output = output.sort_index(ascending=True)
    return output


def net_risk(long_risk_dict, short_risk_dict):
    tickers = set(long_risk_dict.keys()) or set(short_risk_dict.keys())
    output = {}
    for t in tickers:
        if t in long_risk_dict.keys():
            if t in short_risk_dict.keys():
                vega = long_risk_dict[t]["vega"]
                min_ba = long_risk_dict[t]["min_ba"]
                units = long_risk_dict[t]["units"] - short_risk_dict[t]["units"]
                output[t] = pd.Series(index=["vega", "min_ba", "units"], data=[vega, min_ba, units])
            else:
                vega = long_risk_dict[t]["vega"]
                min_ba = long_risk_dict[t]["min_ba"]
                units = long_risk_dict[t]["units"]
                output[t] = pd.Series(index=["vega", "min_ba", "units"], data=[vega, min_ba, units])
        else:
            vega = short_risk_dict[t]["vega"]
            min_ba = short_risk_dict[t]["min_ba"]
            units = -short_risk_dict[t]["units"]
            output[t] = pd.Series(index=["vega", "min_ba", "units"], data=[vega, min_ba, units])

    return output
