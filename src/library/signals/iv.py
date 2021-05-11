from datetime import datetime
from decimal import Decimal

import numpy as np
import pandas as pd

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

vols_table = Table(dbr.Table(db_dict["vols_table"]))
iv_table = Table(dbr.Table(db_dict["iv_table"]))

iv_windows = param_rviv.IV_WINDOWS
list_iv_matus = list(iv_windows.values())

def query_vols(ticker, date=None):
    if date is None:
        return vols_table.query("ticker", ticker)
    else:
        return vols_table.query_range_gt_than("ticker", ticker, "trade_date", date)


def transform_iv_dict(r):
    td = datetime.strptime(r['trade_date'], '%Y-%m-%d')
    iv_dict = dict(((datetime.strptime(k, '%Y-%m-%d') - td).days, v) for (k, v) in r.items() if
                   (k not in ["trade_date", "spot", "ticker"]))
    return dict(sorted(iv_dict.items()))


def interp_matu(t, v1, v2, t1, t2):
    alpha = Decimal((t - t1) / (t2 - t1))
    return float(np.sqrt(v1 ** 2 + alpha * (v2 ** 2 - v1 ** 2)))


def find_interp_matus(list_t, iv_dict):
    listed_matus = iv_dict.keys()
    output = {}
    for t in list_t:
        matus_below = [m for m in listed_matus if m <= t]
        matus_above = [m for m in listed_matus if m > t]
        if ((len(matus_below) == 0) | (len(matus_above) == 0)):
            output[t] = None
        else:
            m_low = max(matus_below)
            m_high = min(matus_above)
            iv_low = iv_dict[m_low]
            iv_high = iv_dict[m_high]
            output[t] = interp_matu(t, iv_low, iv_high, m_low, m_high)
    return output

def find_interp_matu(matu, iv_dict):
    listed_matus = iv_dict.keys()

    matus_below = [m for m in listed_matus if m <= matu]
    matus_above = [m for m in listed_matus if m > matu]
    if ((len(matus_below) == 0) | (len(matus_above) == 0)):
        output = None
    else:
        m_low = max(matus_below)
        m_high = min(matus_above)
        iv_low = iv_dict[m_low]
        iv_high = iv_dict[m_high]
        output = interp_matu(matu, iv_low, iv_high, m_low, m_high)
    return output

def calc_ticker_iv_date(ticker, date= None):
    items = query_vols(ticker, date)
    dd = {}
    for r in items:
        dd[r["trade_date"]] = find_interp_matus(list_iv_matus, transform_iv_dict(r))
        dd[r["trade_date"]]["spot"] = float(r["spot"])
    df = pd.DataFrame.from_dict(dd, orient='index')
    df.rename(columns={v: ("iv_" + k) for (k, v) in iv_windows.items()}, inplace=True)
    df["ticker"] = ticker
    df = df.reset_index().rename(columns={"index": "trade_date"})
    return df



def write_ticker_iv_date(ticker, date=None):
    ivs_df = calc_ticker_iv_date(ticker, date)
    iv_table.put_df_batch(ivs_df)

def get_last_written_date(ticker):
    db_items = iv_table.query("ticker", ticker)
    db_dates = [item["trade_date"] for item in db_items]
    try:
        start_write_index = list_of_dates.index(db_dates[-1])
        return list_of_dates[start_write_index]
    except:
        return None

def batch_write_ivs():
    te_table = Table(dbr.Table(db_dict["vols_table"]))
    tickers = list(set([item["ticker"] for item in te_table.scan()]))
    for t in tickers:
        lw_date = get_last_written_date(t)
        write_ticker_iv_date(t, lw_date)
