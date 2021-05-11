
import pandas as pd

from src.library.dynamo.Client import Client
from src.library.dynamo.Table import Table
from src.library.helpers.dates import get_dates
from src.library.helpers.general import bring_columns_first
from src.library.osmv.Osmv import Osmv
from src.library.params import param
from src.library.params import rviv as param_rviv

osmv = Osmv(param.IS_LOCAL, param.BUCKET_NAME)
(dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.select_env(param.ENV_USED)
(list_of_dates, list_obj_dates) = get_dates()

DB = Client(dbc)
data_table = Table(dbr.Table(db_dict["data_table"]))
vols_table = Table(dbr.Table(db_dict["vols_table"]))
rv_table = Table(dbr.Table(db_dict["rv_table"]))
iv_table = Table(dbr.Table(db_dict["iv_table"]))

iv_windows = param_rviv.IV_WINDOWS
AF = param_rviv.AF

rv_windows = {("rv_" + k): int(AF / 365 * v) for k, v in iv_windows.items()}
rv_cols = list(rv_windows.keys())

def query_rviv(ticker):
    rv = pd.DataFrame(rv_table.query("ticker",ticker)).set_index("trade_date")
    iv = pd.DataFrame(iv_table.query("ticker", ticker)).set_index("trade_date")
    df = pd.concat((rv,iv),axis=1)
    df = df.reset_index().rename(columns={"index": "trade_date"})
    df = df.loc[:, ~df.columns.duplicated()]
    df = bring_columns_first(df, ["ticker","trade_date","spot"])
    for m in iv_windows.keys():
        try:
            df["rviv_" + m] = df["rv_" + m] / df["iv_" + m]
            df["rviv_lag_" + m] = df["rv_" + m] / df["iv_" + m].shift(periods=rv_windows["rv_" + m])
        except:
            pass

    return df


