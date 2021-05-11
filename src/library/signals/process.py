

from src.library.dynamo.Client import Client
from src.library.dynamo.Table import Table

from src.library.osmv.Osmv import Osmv
from src.library.params import param
from src.library.params import rviv as param_rviv
from src.library.signals.calc import calc_signals
from src.library.signals.calc import select_signals
from src.library.signals.rviv import query_rviv

osmv = Osmv(param.IS_LOCAL, param.BUCKET_NAME)
(dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.select_env(param.ENV_USED)


data_table = Table(dbr.Table(db_dict["data_table"]))
vols_table = Table(dbr.Table(db_dict["vols_table"]))
rv_table = Table(dbr.Table(db_dict["rv_table"]))
iv_table = Table(dbr.Table(db_dict["iv_table"]))

def process_signal_ticker(ticker, ref_ticker, maturity_suffix):

    table_name = "signals"+"_"+ref_ticker+"_"+maturity_suffix

    DB = Client(dbc)
    list_tables = DB.list_tables()

    # create table

    if table_name not in list_tables:
        DB.create_table(table_name, "ticker", "S", True, "trade_date", "S")
        DB.add_index(table_name, "reverse", "trade_date", "S", "ticker", "S")

    signal_table = Table(dbr.Table(table_name))

    #batch calc_signals

    selected_signals = select_signals(maturity_suffix, param_rviv.pct_prefix_list, param_rviv.median_prefix_list, param_rviv.proba_1_prefix_list)

    ref_data = query_rviv(ref_ticker)
    df = calc_signals(ticker, ref_data, selected_signals)

    signal_table.put_df_batch(df)

    return df

def process_signal_all(ref_ticker, maturity_suffix):

    table_name = "signals"+"_"+ref_ticker+"_"+maturity_suffix

    DB = Client(dbc)
    list_tables = DB.list_tables()

    # create table

    if table_name not in list_tables:
        DB.create_table(table_name, "ticker", "S", True, "trade_date", "S")
        DB.add_index(table_name, "reverse", "trade_date", "S", "ticker", "S")

    signal_table = Table(dbr.Table(table_name))

    #batch calc_signals

    selected_signals = select_signals(maturity_suffix, param_rviv.pct_prefix_list, param_rviv.median_prefix_list, param_rviv.proba_1_prefix_list)

    ref_data = query_rviv(ref_ticker)

    te_table = Table(dbr.Table(db_dict["vols_table"]))
    tickers = list(set([item["ticker"] for item in te_table.scan()]))

    for t in tickers:
        df = calc_signals(t, ref_data, selected_signals)
        #write them
        signal_table.put_df_batch(df)
