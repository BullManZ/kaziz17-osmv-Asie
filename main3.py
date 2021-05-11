# from src.library.params import param
# from src.library.osmv.Osmv import Osmv
# from src.library.helpers.dates import get_dates
import warnings

from pandas.core.common import SettingWithCopyWarning

#from src.library.backtest.process import process_ts_all

# osmv = Osmv(param.IS_LOCAL, param.BUCKET_NAME)
# (dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.select_env(param.ENV_USED)
# (list_of_dates, list_obj_dates) = get_dates()
from src.library.backtest.process import process_ts_ticker

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)


def batch():
    strikes = [0.9,0.95,1]
    maturity_suffix = ["3m","6m","1y"]
    for m in maturity_suffix:
        process_ts_ticker("ZW", strikes, m, create_tables=False)

if __name__ == '__main__':
    batch()


