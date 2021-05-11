from src.library.backtest.singlename import ticker_bo_ts
from src.library.dynamo.Client import Client
from src.library.dynamo.Table import Table
from src.library.helpers.lists import list_dates
from src.library.helpers.lists import list_tickers

from src.library.osmv.Osmv import Osmv
from src.library.params import param
from src.library.params import timeserie

osmv = Osmv(param.IS_LOCAL, param.BUCKET_NAME)
(dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.select_env(param.ENV_USED)

all_dates = list_dates()

def derive_table_name(strike, maturity_suffix):
    return "ts" + "_" + str(int(100 * strike)) + "_" + maturity_suffix


def process_ts_ticker(ticker, strikes, maturity_suffix, create_tables=False):
    errors_table = Table(dbr.Table("ts_errors"))
    table_names = [derive_table_name(strike, maturity_suffix) for strike in strikes]
    if create_tables:
        DB = Client(dbc)
        list_tables = DB.list_tables()
        for table_name in table_names:
            if table_name not in list_tables:
                DB.create_table(table_name, "ticker", "S", True, "trade_date", "S")
                DB.add_index(table_name, "reverse", "trade_date", "S", "ticker", "S")

    # batch calc_store_ts
    try:
        output, _ = ticker_bo_ts(ticker, strikes, maturity_suffix, all_dates[0], all_dates[-1],
                                 timeserie.strike_change_threshold)
        for i, strike in enumerate(strikes):
            try:
                output[strike]["ticker"] = ticker
                ts_table = Table(dbr.Table(table_names[i]))
                ts_table.put_df_batch(output[strike])
            except Exception as ee:
                errors_table.put_item(
                    {"ticker": ticker, "error": str(strike), "exception": str(ee)})
                # print(str(ee))

    except Exception as e:
        errors_table.put_item(
            {"ticker": ticker, "error": "ts_calc", "exception":str(e)})
        # print(str(e))


def process_ts_all(strikes, maturity_suffix):
    tickers = list_tickers()

    for i, ticker in enumerate(tickers):
        if i == 0:
            process_ts_ticker(ticker, strikes, maturity_suffix, create_tables=True)
        else:
            process_ts_ticker(ticker, strikes, maturity_suffix, create_tables=False)
