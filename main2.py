# from src.library.params import param
# from src.library.osmv.Osmv import Osmv
# from src.library.helpers.dates import get_dates
import warnings
import pandas as pd
from pandas.core.common import SettingWithCopyWarning

# from src.library.backtest.process import process_ts_all

# osmv = Osmv(param.IS_LOCAL, param.BUCKET_NAME)
# (dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.select_env(param.ENV_USED)
# (list_of_dates, list_obj_dates) = get_dates()
# from src.library.backtest.process import process_ts_ticker
from src.library.importer.importer import read_df_csv_s3, batch_process
from src.library.importer.tweak import tweak, calc_pnl

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)



# def batch():
#     strikes = [0.9,0.95,1]
#     maturity_suffix = ["3m","6m","1y"]
#     for m in maturity_suffix:
#         process_ts_ticker("SPX", strikes, m, create_tables=False)
#
# if __name__ == '__main__':
#     batch()


from src.library.importer import importer

batch_process(0, 2)