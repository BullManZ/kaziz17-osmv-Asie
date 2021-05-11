from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

import src.library.params.param as param
import src.library.params.timeserie as timeserie
from src.library.backtest.stocksplit import disappeared_ref
from src.library.backtest.stocksplit import lookup_ss
from src.library.backtest.stocksplit import stock_split_ref
from src.library.backtest.strikes import select_refs_for_strikes
from src.library.dynamo.Table import Table
from src.library.helpers.dates import date_intervals
#from src.library.helpers.dates import get_dates
from src.library.helpers.dates import next_date
from src.library.helpers.dates import rebal_fixed_strike
from src.library.helpers.general import bring_columns_first
from src.library.helpers.general import df_decimal_to_float
from src.library.helpers.general import null
from src.library.osmv.Osmv import Osmv
from src.library.params import columns

osmv = Osmv(param.IS_LOCAL, param.BUCKET_NAME)
(dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.select_env(param.ENV_USED)

list_obj_dates = []
list_of_dates = []

# paginator = s3c.get_paginator('list_objects_v2')
# pages = paginator.paginate(Bucket='osmv-2', Prefix='prefix')
# for page in pages:
for obj in bucket.objects.all():
    tmp = obj.key.encode('utf-8')[-12:-4]
    list_obj_dates.append(tmp)

for elt in range(len(list_obj_dates)):
    var = list_obj_dates[elt]
    tmp = (datetime.datetime.strptime(var.decode("utf-8")), "%Y%m%d").date()
    list_of_dates.append(tmp)

#quandl_rebal_dates = rebal_fixed_strike(list_of_dates)

data_table = Table(dbr.Table(db_dict["data_table"]))

threshold_default = timeserie.strike_change_threshold

keep_cols = timeserie.KEEP_COLS
common_cols = timeserie.COMMON_TS_COLS
expi_cols = timeserie.EXPI_TS_COLS
alpha_cols = timeserie.ALPHA_TS_COLS
base_cols = timeserie.BASE_COLS

expi_threshold = timeserie.expi_threshold
max_distance_one_expi = timeserie.max_distance_one_expi
delta_threshold = timeserie.delta_threshold
bidask_cap = timeserie.bidask_cap



def select_expis(df, start_date, maturity_string):
    expis = list(df.expi.unique())
    expis = [e for e in expis if e in list_of_dates]

    sd = datetime.strptime(start_date, '%Y-%m-%d')

    if maturity_string[1] == "m":
        rd = relativedelta(months=+int(maturity_string[0]))
    elif maturity_string[1] == "y":
        rd = relativedelta(years=+int(maturity_string[0]))
    else:
        raise ValueError("Error: maturity format")

    expi = sd + rd

    expi_dates = [datetime.strptime(d, '%Y-%m-%d') for d in expis]

    def absolute_date_diff(list_value):
        return abs(list_value - expi)

    closest_expi = min(expi_dates, key=absolute_date_diff)
    closest_distance = abs((closest_expi - expi).days / (sd - expi).days)
    closest_expi_string = closest_expi.strftime('%Y-%m-%d')

    if closest_distance < expi_threshold:
        return [closest_expi_string]
    else:
        if closest_expi > expi:
            selected_expis = [d for d in expi_dates if d <= expi]
        else:
            selected_expis = [d for d in expi_dates if d >= expi]

        if len(selected_expis) == 0:
            if closest_distance > max_distance_one_expi:
                return []
            else:
                return [closest_expi_string]

        second_expi = min(selected_expis, key=absolute_date_diff)
        second_expi_string = second_expi.strftime('%Y-%m-%d')
        min_expi = min(closest_expi_string, second_expi_string)
        max_expi = max(closest_expi_string, second_expi_string)
        return [min_expi, max_expi]


def transform_df_expi(df, maturity_string):
    if maturity_string[1] == "m":
        matu_lambda = lambda x: datetime.strptime(x, '%Y-%m-%d') + relativedelta(months=+int(maturity_string[0]))
    elif maturity_string[1] == "y":
        matu_lambda = lambda x: datetime.strptime(x, '%Y-%m-%d') + relativedelta(years=+int(maturity_string[0]))
    else:
        raise ValueError("Error: maturity string")

    output = df
    output["theo_expi"] = output["trade_date"].apply(matu_lambda)
    output["expi_dt"] = output["expi"].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    try:
        return output[common_cols + expi_cols + alpha_cols].reset_index().drop(["index"], axis=1)
    except:
        print('here')


def check_deltas(df, delta_threshold):
    above = (df.delta < delta_threshold).sum() > 0
    below = (df.delta > (1 - delta_threshold)).sum() > 0
    keep_df = ~above & ~below
    return keep_df


def two_expis_to_df(dict_expis, maturity_string):
    tr_dfs = [transform_df_expi(df, maturity_string) for df in dict_expis.values()]

    keep_df0 = check_deltas(tr_dfs[0], delta_threshold)
    keep_df1 = check_deltas(tr_dfs[1], delta_threshold)

    if keep_df0 & keep_df1:

        df_maturity = tr_dfs[0][common_cols]

        for col in expi_cols:
            df_maturity[col + "_0"] = tr_dfs[0][col]
            df_maturity[col + "_1"] = tr_dfs[1][col]

        # alpha to be checked with Arnaud
        df_maturity["alpha_matu"] = (df_maturity.theo_expi - df_maturity.expi_dt_0) / (
                df_maturity.expi_dt_1 - df_maturity.expi_dt_0)

        for col in alpha_cols:
            df_maturity[col + "_0"] = tr_dfs[0][col]
            df_maturity[col + "_1"] = tr_dfs[1][col]
            df_maturity[col] = tr_dfs[0][col] * df_maturity["alpha_matu"] + tr_dfs[1][col] * (
                    1 - df_maturity["alpha_matu"])

        df_maturity.theo_expi = df_maturity.theo_expi.apply(lambda dt: dt.strftime('%Y-%m-%d'))
        df_maturity["min_ba"] = tr_dfs[0]["min_ba"] * df_maturity["alpha_matu"] + tr_dfs[1]["min_ba"] * (
                1 - df_maturity["alpha_matu"])
        df_maturity.drop(["expi_dt_0", "expi_dt_1"], axis=1, inplace=True)

        return df_maturity

    elif keep_df0:
        return one_expi_to_df(tr_dfs[0], maturity_string)
    else:
        return one_expi_to_df(tr_dfs[1], maturity_string)


def one_expi_to_df(df, maturity_string):
    df_maturity = transform_df_expi(df, maturity_string)
    df_maturity.theo_expi = df_maturity.theo_expi.apply(lambda dt: dt.strftime('%Y-%m-%d'))
    df_maturity.drop(["expi_dt"], axis=1, inplace=True)
    return df_maturity


def load_df(start_date, ticker, expi):
    df = pd.DataFrame(
        data_table.query_index_range_begins_with("reverse", "trade_date", start_date, "ref", ticker + "-" + expi))
    df = df_decimal_to_float(df)
    df = bring_columns_first(df, ["trade_date", "ref", "cdh_pnl"])
    if "cdh_pnl" not in df.columns:
        df[columns.pnl_cols] = 0
    return df


def base_bo_ts(start_date, end_date, ticker, expi, strikes, threshold, export_dfs=False):
    exports = {}
    all_refs = []
    df = load_df(start_date, ticker, expi)

    if len(df) > 0:
        dict_refs = select_refs_for_strikes(df, strikes)
    else:
        raise ValueError("Error: No data found in DB for ticker " + ticker + ", expi " + expi)

    n_date = next_date(start_date, list_of_dates)

    if export_dfs:
        for ref in dict_refs.values():
            all_refs.append(ref)
        for ref in all_refs:
            sheet_key = ref + start_date
            meta_dict = {
                "ticker": ticker,
                "ref": ref,
                "expiry": expi,
                "start date": start_date,
                "end_date": end_date
            }
            sheet_key = sheet_key[:31]
            exports[sheet_key] = {"df": df[df.ref == ref], "meta": meta_dict}

    new_refs = dict_refs

    spot = df.spot.iloc[0]
    (day_dfs, dict_refs, break_loop) = extract_refs(ticker, expi, start_date, strikes, df, dict_refs)
    for k in strikes:
        day_dfs[k] = compute_vega_diff(day_dfs[k], day_dfs[k])

    spots = [spot]
    dict_dfs = day_dfs

    end = min(end_date, expi)

    while n_date <= end:

        date = n_date
        prev_df = df
        df = load_df(date, ticker, expi)

        if len(df) > 0:

            spot_t = df.spot.iloc[0]
            spot_move = abs(spot_t / spot - 1)

            if (date in quandl_rebal_dates) | (spot_move > threshold):
                rebal = True
                spot = spot_t
            else:
                rebal = False

            if export_dfs:
                for ref in dict_refs.values():
                    all_refs.append(ref)
                all_refs = list(set(all_refs))
                for ref in all_refs:
                    sheet_key = ref + start_date
                    meta_dict = {
                        "ticker": ticker,
                        "ref": ref,
                        "expiry": expi,
                        "start date": start_date,
                        "end_date": end_date
                    }
                    sheet_key = sheet_key[:31]
                    if sheet_key in exports:
                        exports[sheet_key] = {"df": pd.concat((exports[sheet_key]["df"], df[df.ref == ref]), axis=0),
                                              "meta": meta_dict}
                    else:
                        exports[sheet_key] = {"df": df[df.ref == ref], "meta": meta_dict}

            if rebal:
                new_refs = select_refs_for_strikes(df, strikes)

            (day_dfs, dict_refs, break_loop) = extract_refs(ticker, expi, date, strikes, df, dict_refs, prev_df)
            if break_loop:
                break

            for k in strikes:
                df_new_ref = df[df.ref == new_refs[k]]
                day_dfs[k] = compute_vega_diff(day_dfs[k], df_new_ref)
                dict_dfs[k] = pd.concat((dict_dfs[k], day_dfs[k]), axis=0)
            spots.append(spot_t)

        dict_refs = new_refs
        n_date = next_date(date, list_of_dates)

    return dict_dfs, exports


def compute_vega_diff(day_df, df_new_ref):
    df_ref = day_df.copy()
    df_ref["strike_vega_change"] = abs(df_ref.vega.iloc[0] - df_new_ref.vega.iloc[0])
    df_ref["strike_roll_cost"] = df_ref.apply(
        lambda row: min(row["min_ba"], bidask_cap) * row["strike_vega_change"] * 50, axis=1)

    return df_ref


def extract_refs(ticker, expi, date, strikes, df, dict_refs, prev_df=None):
    output = {}
    break_loop = False
    for k in strikes:

        df_k = df[df.ref == dict_refs[k]]

        if len(df_k) == 0:
            # ref disappeared

            (is_ss, stock_split) = lookup_ss(ticker, date)

            if is_ss:
                (df_k, adj_strike) = stock_split_ref(df, stock_split, dict_refs[k], prev_df)
                dict_refs[k] = ticker + "-" + expi + "-" + str(adj_strike)

            else:
                df_k = disappeared_ref(date, dict_refs[k])
                break_loop = True

        output[k] = df_k

    return output, dict_refs, break_loop


def ticker_bo_ts(ticker, strikes, maturity_string, start_date, end_date, threshold, export_dfs=False ):
    strikes_dfs = {}
    exports = {}

    for k in strikes:
        strikes_dfs[k] = None

    rebal_dates = date_intervals(start_date, end_date, quandl_rebal_dates)

    for i in range(len(rebal_dates) - 1):

        keep_date = True

        # select expis
        df = pd.DataFrame(
            data_table.query_index_range_begins_with("reverse", "trade_date", rebal_dates[i], "ref", ticker+"-"))

        if len(df) > 0:

            expis = select_expis(df, rebal_dates[i], maturity_string)
            num_expis = len(expis)

            all_strikes_ts_dict = {}

            for expi in expis:
                (all_strikes_ts_dict[expi], base_ts_exports) = base_bo_ts(rebal_dates[i], rebal_dates[i + 1], ticker,
                                                                          expi, strikes, threshold,
                                                                          export_dfs=export_dfs)
                exports.update(base_ts_exports)

            for k in strikes:

                specific_strike_ts_dict = {}
                for expi in expis:
                    specific_strike_ts_dict[expi] = all_strikes_ts_dict[expi][k]

                    if export_dfs:
                        if i == 0:
                            sheet_key = ticker + "_main_expi_k=" + str(k) + "_" + rebal_dates[i]
                        else:
                            sheet_key = ticker + "_2nd_expi_k=" + str(k) + "_" + rebal_dates[i]

                        meta_dict = {
                            "ticker": ticker,
                            "strike": k,
                            "expiry": expi,
                            "start date": rebal_dates[i],
                            "end_date": rebal_dates[i + 1]
                        }
                        sheet_key = sheet_key[:31]
                        exports[sheet_key] = {"df": all_strikes_ts_dict[expi][k], "meta": meta_dict}

                if len(expis) == 2:
                    df_maturity = two_expis_to_df(specific_strike_ts_dict, maturity_string)
                elif len(expis) == 1:
                    df_maturity = one_expi_to_df(specific_strike_ts_dict[expis[0]], maturity_string)
                else:
                    keep_date = False

                if keep_date:
                    start_index = 1 * (i > 0)
                    strikes_dfs[k] = pd.concat((strikes_dfs[k], df_maturity[start_index:]), axis=0)


    strikes_dfs = add_expi_roll_cost(strikes_dfs)
    return strikes_dfs, exports


def add_expi_roll_cost(strikes_dfs):
    for (k,df) in strikes_dfs.items():
        if df is not None:
            df["expi_roll_cost"]=0
            for i in range(1,len(df)):
               df["expi_roll_cost"].iloc[i] =  diff_expi_cbls(expi_cbl_df(df.iloc[i]), expi_cbl_df(df.iloc[i-1]))
            df["trans_cost"] = df["expi_roll_cost"] + df["strike_roll_cost"]
            df["gross_pnl"] = df["cdh_pnl"].cumsum()
            df["day_net_pnl"] = df["cdh_pnl"] - df["trans_cost"]
            df["net_pnl"] = df["day_net_pnl"].cumsum()
    return strikes_dfs

def expi_cbl_df(row):
    if "alpha_matu" in row.index:
        if null(row["alpha_matu"]):
            data = {"alpha_matu":[1], "vega":[row["vega"]], "min_ba" : [row["min_ba"]]}
            index = ["expi"]
        else:
            data = {"alpha_matu":[1- row["alpha_matu"], row["alpha_matu"]], "vega":[row["vega_0"],row["vega_1"]], "min_ba" : [row["min_ba_0"],row["min_ba_1"]]}
            index = ["expi_0", "expi_1"]
    else:
        data = {"alpha_matu": [1], "vega": [row["vega"]], "min_ba": [row["min_ba"]]}
        index = ["expi"]

    return pd.DataFrame(index=index, data=data)


def diff_expi_cbls(df1, df2):
    if ("expi" in df1.index) & ("expi" in df2.index):
        cost = 0
    else:
        max_vega = max(max(df1.vega), max(df2.vega))
        min_vega = min(min(df1.vega), min(df2.vega))
        max_ba = max( max(df1.min_ba), max(df2.min_ba))
        if ("expi_0" in df1.index) & ("expi_0" in df2.index):
            d_alpha = abs(df1.alpha_matu.iloc[0] - df2.alpha_matu.iloc[0])
            cost = d_alpha*(max_vega - min_vega)*min(max_ba, bidask_cap)*50
        else:
            d_alpha = 0.5
            cost =  d_alpha*(max_vega - min_vega)*min(max_ba, bidask_cap)*50
    return cost