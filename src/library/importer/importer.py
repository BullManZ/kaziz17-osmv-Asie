import pandas as pd

import src.library.params.columns as columns
import src.library.params.import_param as import_param
import src.library.params.param as param
from src.library.dynamo.Table import Table
from src.library.helpers.dates import get_dates
from src.library.helpers.general import df_decimal_to_float
from src.library.importer.filters import filter_first_refs
from src.library.importer.filters import filter_new_refs
from src.library.importer.tweak import calc_pnl
from src.library.importer.tweak import tweak
from src.library.osmv.Osmv import Osmv
from src.library.signals.vol import populate_expis

osmv = Osmv(param.IS_LOCAL, param.BUCKET_NAME)
(dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.select_env(param.ENV_USED)
(list_of_dates, list_obj_dates) = get_dates()

data_table = Table(dbr.Table(db_dict["data_table"]))
vols_table = Table(dbr.Table(db_dict["vols_table"]))
refs_table = Table(dbr.Table(db_dict["state_refs_table"]))
te_table = Table(dbr.Table(db_dict["ticker_expis_table"]))
errors_table = Table(dbr.Table(db_dict["errors_table"]))

import logging
import tracemalloc

log_format = "%(asctime)s::%(levelname)s::%(name)s::" \
             "%(filename)s::%(lineno)d::%(message)s"
logging.basicConfig(filename='logs.log', level='INFO', format=log_format)

logger = logging.getLogger('server_logger')

from datetime import datetime


def snap_memory():
    tracemalloc.start(25)
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics('traceback')

    # pick the biggest memory blocks
    for stat in top_stats[:3]:
        print("%s memory blocks: %.1f KiB" % (stat.count, stat.size / 1024))
        for line in stat.traceback.format():
            logger.info(line)


def read_df_csv_s3(key, bucket_name, dtype_dict, names):
    # Create a file object using the bucket and object key.
    fileobj = s3c.get_object(
        Bucket=bucket_name,
        Key=key
    )
    # open the file object and read it into the variable filedata.
    return pd.read_csv(fileobj['Body'], dtype=dtype_dict, names=names, low_memory=False)


def process_first_day_idx(idx):
    key = "Asia_RawIV_EOD_" + list_obj_dates[idx] + ".csv"
    date = list_of_dates[idx]
    column_list = ['date', 'symbol', 'exchange', 'company_name', 'stock_price_close', 'option_symbol',
                   'expirDate', 'strike', 'call_put', 'style', 'open', 'high', 'low', 'close', 'bid', 'ask',
                   'mean_price', 'settlement', 'iv', 'volume', 'open_interest', 'stock_price_for_iv',
                   'forward_price', 'isinterpolated', 'delta', 'vega', 'gamma', 'theta', 'rho']

    df = read_df_csv_s3(key, param.BUCKET_NAME, import_param.DTYPE_DICT, names=column_list)

    df = tweak(df)

    dt = None

    df = filter_first_refs(df, import_param.filtering_dict)

    # no new refs, therefore
    df_new = df
    # write data to DB
    data_table.put_df_batch(df_new)
    # write ticker-expis table
    write_ticker_expis(df_new, date)
    # write state refs
    added = len(df_new)
    existing = 0
    write_state_refs(date, existing, added)
    # write state refs
    df = pd.concat((dt, df_new), axis=0)
    write_vols(df, date)

    return df


def log_msg(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    logging.warning(msg + " " + ts)


def stream_process_day(idx, prev_day_df=None):
    key = "Asia_RawIV_EOD_" + list_obj_dates[idx] + ".csv"
    date = list_of_dates[idx]

    column_list = ['date', 'symbol', 'exchange', 'company_name', 'stock_price_close', 'option_symbol',
                   'expirDate', 'strike', 'call_put', 'style', 'open', 'high', 'low', 'close', 'bid', 'ask',
                   'mean_price', 'settlement', 'iv', 'volume', 'open_interest', 'stock_price_for_iv',
                   'forward_price', 'isinterpolated', 'delta', 'vega', 'gamma', 'theta', 'rho']

    df = read_df_csv_s3(key, param.BUCKET_NAME, import_param.DTYPE_DICT, names = column_list)

    df = tweak(df)

    if prev_day_df is not None:
        current_refs = set(prev_day_df.ref)
        df = (filter_new_refs(date, df, import_param.filtering_dict, prev_day_df))
        df_current = df[df.ref.isin(current_refs)]
        df_new = df[~df.ref.isin(current_refs)]

    else:
        prev_day_data = data_table.query_index("reverse", "trade_date", list_of_dates[idx - 1])
        current_refs = [item["ref"] for item in prev_day_data]
        prev_day_df = df_decimal_to_float(pd.DataFrame(prev_day_data))
        df = (filter_new_refs(date, df, import_param.filtering_dict, prev_day_df))
        df_current = df[df.ref.isin(current_refs)]
        df_new = df[~df.ref.isin(current_refs)]

    # calc_pnl , add existing refs to DB
    dt = calc_pnl(df_current, prev_day_df)

    dt_write = dt[columns.all_cols + columns.pnl_cols]
    data_table.put_df_batch(dt_write)

    existing = len(current_refs)

    # add new refs to DB
    data_table.put_df_batch(df_new)

    write_ticker_expis(df_new, date)

    added = len(df_new)
    write_state_refs(date, existing, added)

    df = pd.concat((dt, df_new), axis=0)
    write_vols(df, date)

    return df


def batch_process(start_idx, end_idx):
    if start_idx == 0:
        prev_day_df = process_first_day_idx(start_idx)
    else:
        prev_day_df = stream_process_day(start_idx)

    for idx in range(start_idx + 1, end_idx):
        prev_day_df = stream_process_day(idx, prev_day_df)
        #print(list_of_dates[idx])
        if idx == 100:
            snap_memory()

    return prev_day_df


def get_last_written_date(env=param.ENV_USED):
    db_items = refs_table.query("env", env)
    db_dates = [item["trade_date"] for item in db_items]
    start_write_index = list_of_dates.index(db_dates[-1])
    return start_write_index


def write_vols(df, date):
    df_vols = populate_expis(df, date)
    vols_table.put_df_batch(df_vols)


def write_ticker_expis(df_new, date):
    df_ticker_expi = df_new.groupby(["ticker", "expi"]).first().reset_index()
    df_ticker_expi = df_ticker_expi[["ticker", "expi"]]
    df_ticker_expi["ticker-expi"] = df_ticker_expi["ticker"] + df_ticker_expi["expi"]
    df_ticker_expi["env"] = param.ENV_USED
    df_ticker_expi["date_added"] = date
    te_table.put_df_batch(df_ticker_expi)


def write_state_refs(date, existing, added):
    refs_table.put_item({"env": param.ENV_USED,
                         "trade_date": str(date),
                         "existing": existing,
                         "added": added,
                         })
