from decimal import Decimal

import pandas as pd

from src.library.backtest.greekneutral import greek_neutral_ts
from src.library.backtest.stockpicker import select_signal_sequence
from src.library.helpers.dates import backtest_rebal_dates_monthly
from src.library.helpers.dates import backtest_rebal_dates_weekly
from src.library.helpers.dates import date_intervals
from src.library.helpers.dates import get_dates
from src.library.params import timeserie

(list_of_dates, list_obj_dates) = get_dates()

bidask_cap = timeserie.bidask_cap

dict_freq = {"weekly": backtest_rebal_dates_weekly(list_of_dates),
             "monthly": backtest_rebal_dates_monthly(list_of_dates)}


# def backtest_sequence(greek, strike, start, end, maturity_string, signal_cols, ref_ticker, list_num_stocks,
#                       list_largest, threshold, rebal_dates_freq, export_dfs=False):
#     exports = {}
#     exceptions = {}
#     rebal_dates_array = dict_freq[rebal_dates_freq]
#     rebal_dates = date_intervals(start, end, rebal_dates_array)
#
#     output = None
#
#     table_name = "signals" + "_" + ref_ticker + "_" + maturity_string
#
#     for i in range(0, len(rebal_dates) - 1):
#         stocks_long = select_signal_sequence(rebal_dates[i], table_name, signal_cols, list_num_stocks, list_largest)
#
#         stocks_short = [ref_ticker]
#
#         (df, ticker_ts_exports, gn_exceptions) = greek_neutral_ts(greek, strike, rebal_dates[i], rebal_dates[i + 1],
#                                                                   maturity_string, stocks_long, stocks_short, threshold,
#                                                                   export_dfs=True)
#
#         exceptions.update(gn_exceptions)
#
#         if export_dfs:
#             exports.update(ticker_ts_exports)
#
#             sheet_key = greek + "_" + maturity_string + "_k=" + str(strike) + "_p=" + str(i)
#             meta_dict = {
#                 "greek": greek,
#                 "rolling maturity": maturity_string,
#                 "strike": strike,
#                 "start date": rebal_dates[i],
#                 "end date": rebal_dates[i + 1],
#                 "threshold": threshold}
#
#             exports[sheet_key] = {"df": df, "meta": meta_dict}
#
#         df["stocks_long"] = '-'.join(stocks_long)
#         df["stocks_short"] = '-'.join(stocks_short)
#         units_columns = [col for col in df.columns if col[:5] == "units"]
#         df.drop(units_columns, axis=1, inplace=True)
#
#         start_index = 1 * (i > 0)
#         output = pd.concat((output, df[start_index:]), axis=0)
#
#     output["gross_pnl"] = output["cdh_pnl"].cumsum()
#     output["net_pnl"] = output["cdh_pnl"].cumsum() - output["trans_cost"].cumsum()
#     output.loc[output["trade_date"] == start, "gross_pnl"] = 0
#     output.loc[output["trade_date"] == start, "net_pnl"] = 0
#
#     if export_dfs:
#         meta_dict = {"greek": greek,
#                      "rolling maturity": maturity_string,
#                      "strike": strike,
#                      "signals": '-'.join(signal_cols),
#                      "ref_ticker": ref_ticker,
#                      "num stocks per signal layer": '-'.join([str(n) for n in list_num_stocks]),
#                      "largest if true": '-'.join([str(n) for n in list_largest])
#                      }
#         exports["backtest"] = {"df": output, "meta": meta_dict}
#
#     return (output, exports, exceptions)


def backtest_bo(greek, strike, start, end, maturity_string, signal_cols, ref_ticker, list_num_stocks,
                list_largest, rebal_dates_freq, excluding_list, export_dfs=False):
    exports = {}
    rebal_dates_array = dict_freq[rebal_dates_freq]
    rebal_dates = date_intervals(start, end, rebal_dates_array)

    output = None

    signals_table_name = "signals" + "_" + ref_ticker + "_" + maturity_string

    prev_end_risk_dict = {}

    for i in range(0, len(rebal_dates) - 1):
        stocks_long = select_signal_sequence(rebal_dates[i], signals_table_name, signal_cols, list_num_stocks,
                                             list_largest, excluding_list)

        stocks_short = [ref_ticker]

        (df, start_risk_dict, end_risk_dict, longs, shorts, gn_exports) = greek_neutral_ts(greek,
                                                                                        strike,
                                                                                        rebal_dates[i],
                                                                                        rebal_dates[i + 1],
                                                                                        maturity_string,
                                                                                        stocks_long,
                                                                                        stocks_short, export_dfs)

        df["stocks_long"] = '-'.join(longs)
        df["stocks_short"] = '-'.join(shorts)

        costs = calc_io_costs(prev_end_risk_dict, start_risk_dict)
        df["io_cost"] = 0
        df["io_cost"].iloc[1] = sum(costs.values())

        if export_dfs:
            exports.update(gn_exports)
            sheet_key = greek + "_" + maturity_string + "_k=" + str(strike) + "_p=" + str(i)
            meta_dict = {
                "greek": greek,
                "rolling maturity": maturity_string,
                "strike": strike,
                "start date": rebal_dates[i],
                "end date": rebal_dates[i + 1]}

            exports[sheet_key] = {"df": df.reset_index().rename(columns={"index": "trade_date"}), "meta": meta_dict}

        prev_end_risk_dict = end_risk_dict

        start_index = 1 * (i > 0)
        output = pd.concat((output, df[start_index:]), axis=0)

    if export_dfs:
        meta_dict = {"greek": greek,
                     "rolling maturity": maturity_string,
                     "strike": strike,
                     "signals": '-'.join(signal_cols),
                     "ref_ticker": ref_ticker,
                     "num stocks per signal layer": '-'.join([str(n) for n in list_num_stocks]),
                     "largest if true": '-'.join([str(n) for n in list_largest])
                     }
        exports["backtest"] = {"df": output.reset_index().rename(columns={"index": "trade_date"}), "meta": meta_dict}

    return output, exports


def calc_io_costs(dict1, dict2):
    tickers = set(dict1.keys()) or set(dict2.keys())
    output = {}
    for t in tickers:
        if t in dict1.keys():
            if t in dict2.keys():
                delta_units = abs(dict1[t]["units"] - dict2[t]["units"])
                cost = delta_units * dict1[t]["vega"] * min(dict1[t]["min_ba"], Decimal(bidask_cap)) * 50
            else:
                delta_units = abs(dict1[t]["units"])
                cost = delta_units * dict1[t]["vega"] * min(dict1[t]["min_ba"], Decimal(bidask_cap)) * 50
        else:
            delta_units = abs(dict2[t]["units"])
            cost = delta_units * dict2[t]["vega"] * min(dict2[t]["min_ba"], Decimal(bidask_cap)) * 50
        output[t] = float(cost)

    return output

#
# def backtest2(greek, strike, start, end, maturity_string, signal_dfs, signal_cols, list_num_stocks, list_largest,
#               threshold, rebal_dates_array):
#     rebal_dates = date_intervals(start, end, rebal_dates_array)
#     rebal_dates_day_after = [next_date(d, list_of_dates) for d in rebal_dates]
#     output = None
#
#     for i in range(0, len(rebal_dates) - 1):
#         stocks_long = select_signal_sequence(rebal_dates[i], signal_dfs, signal_cols, list_num_stocks, list_largest)
#
#         short_order = [not x for x in list_largest]
#         stocks_short = ["select_signal_sequence(rebal_dates[i],signal_dfs,signal_cols,list_num_stocks,short_order)"]
#
#         df = greek_neutral_ts(greek, strike, rebal_dates[i], rebal_dates[i + 1], maturity_string, stocks_long,
#                               stocks_short, threshold)
#         df["stocks_long"] = '-'.join(stocks_long)
#         df["stocks_short"] = '-'.join(stocks_short)
#
#         start_index = 1 * (i > 0)
#         output = pd.concat((output, df[start_index:]), axis=0)
#
#     output["strat_pnl"] = output["cdh_pnl"].cumsum()
#     output.loc[output["trade_date"] == start, "strat_pnl"] = 0
#     return output
