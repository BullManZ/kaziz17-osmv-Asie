from datetime import datetime

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

import src.library.params.param as param
from src.library.dynamo.Client import Client
from src.library.dynamo.Table import Table
from src.library.helpers.dates import get_dates
from src.library.osmv.Osmv import Osmv
from src.library.params import rviv as param_rviv

osmv = Osmv(param.IS_LOCAL, param.BUCKET_NAME)
(dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.select_env(param.ENV_USED)
(list_of_dates, list_obj_dates) = get_dates()

DB = Client(dbc)
data_table = Table(dbr.Table(db_dict["data_table"]))
vols_table = Table(dbr.Table(db_dict["vols_table"]))
rv_table = Table(dbr.Table(db_dict["rv_table"]))

iv_windows = param_rviv.IV_WINDOWS
AF = param_rviv.AF

rv_windows = {("rv_" + k): int(AF / 365 * v) for k, v in iv_windows.items()}
rv_cols = list(rv_windows.keys())


def query_vols(ticker, date=None):
    if date is None:
        return vols_table.query("ticker", ticker)
    else:
        return vols_table.query_range_gt_than("ticker", ticker, "trade_date", date)


def realised_vol(spots, AF):
    num_returns = len(spots) - 1
    sq_log_returns = [np.log(float(spots[i + 1] / spots[i])) ** 2 for i in range(num_returns)]
    return np.sqrt(AF / num_returns * sum(sq_log_returns))


def calc_rv(items):
    output = []
    for days, r in enumerate(items):
        day_rvs = {"trade_date": r["trade_date"]}
        for col, lag in rv_windows.items():
            if days >= lag:
                window = [r["spot"] for r in items[(days - lag):(days + 1)]]
                day_rvs[col] = realised_vol(window, AF)
            else:
                day_rvs[col] = None
        output.append(day_rvs)
    return output


def calc_ticker_rv_date(ticker, date=None):

    if date is None:
        items = query_vols(ticker, date=None)
        output = pd.DataFrame(calc_rv(items))
    else:
        date_offset = max(rv_windows.values()) + 2
        dt = datetime.strptime(date, '%Y-%m-%d')
        sdt = dt - relativedelta(days=date_offset)
        start = sdt.strftime('%Y-%m-%d')
        items = query_vols(ticker, date=start)
        rvs = calc_rv(items)

        output = pd.DataFrame([rv for rv in rvs if rv["trade_date"] >= date])

    output["ticker"] = ticker
    return output


def write_ticker_rv_date(ticker, date=None):
    rvs_df = calc_ticker_rv_date(ticker, date)
    rv_table.put_df_batch(rvs_df)


def get_last_written_date(ticker):
    db_items = rv_table.query("ticker", ticker)
    db_dates = [item["trade_date"] for item in db_items]
    try:
        start_write_index = list_of_dates.index(db_dates[-1])
        return list_of_dates[start_write_index]
    except:
        return None

def batch_write_rvs():
    te_table = Table(dbr.Table(db_dict["vols_table"]))
    tickers = list(set([item["ticker"] for item in te_table.scan()]))
    for t in tickers:
        lw_date = get_last_written_date(t)
        write_ticker_rv_date(t, lw_date)
