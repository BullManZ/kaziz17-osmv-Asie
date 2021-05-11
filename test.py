# import logging
#
# import pandas as pd
#
# from src.library.backtest.backtester import backtest_bo
# from src.library.backtest.greekneutral import greek_neutral_ts
# from src.library.backtest.multiname import greek_neutral_ts
# from src.library.backtest.singlename import ticker_bo_ts
from src.library.dynamo.Client import Client
# from src.library.dynamo.Table import Table
# from src.library.helpers.lists import list_dates
from src.library.osmv.Osmv import Osmv
from src.library.params import param
#
osmv = Osmv(param.IS_LOCAL, param.BUCKET_NAME)
# # (dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.select_env(param.ENV_USED)
# list_of_dates = list_dates()
#
# from pandas.core.common import SettingWithCopyWarning
# import warnings
#
# warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)



# data_table = Table(dbr.Table(db_dict["data_table"]))
# stock_splits_table = Table(dbr.Table(db_dict["splits_table"]))
# rv_table = Table(dbr.Table(db_dict["rv_table"]))
# errors_table = Table(dbr.Table(db_dict["errors_table"]))



if __name__ == '__main__':
    # df = pd.DataFrame(
    #     data_table.query_index_range_begins_with("reverse", "trade_date", dd, "ref", ticker + "-"))
    # items = (Table(dbr.Table("ts_90_3m")).scan())
    # tickers = sorted(set([i["ticker"] for i in items]))
    # print(tickers)
    # tickers = ['A', 'AAPL', 'ABX', 'ACN', 'ADBE', 'ADM', 'ADSK', 'AEM']
    # DB = Client(dbc)
    # DB.create_table("data_test2","ref","S",True,"trade_date","S")
    (dbr, dbc, s3r, s3c, bucket) = osmv.init_aws_resources()
    DB = Client(dbc)
    DB.create_table("envs","name","S")
    print(DB.list_tables())
    DB.create_table("dataset", "ref", "S", True, "trade_date", "S")
    print(DB.list_tables())

