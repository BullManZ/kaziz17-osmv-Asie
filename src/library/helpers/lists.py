from src.library.dynamo.Table import Table
from src.library.osmv.Osmv import Osmv
from src.library.params import param

osmv = Osmv(param.IS_LOCAL, param.BUCKET_NAME)
(dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.select_env(param.ENV_USED)

state_refs_table = Table(dbr.Table(db_dict["state_refs_table"]))
te_table = Table(dbr.Table(db_dict["ticker_expis_table"]))


def list_tickers(expi=None):
    if expi:
        items = te_table.query_index("env-expi-index", "env", param.ENV_USED, True, "expi", expi)
    else:
        items = te_table.query_index("env-expi-index", "env", param.ENV_USED)

    return sorted(set([item["ticker"] for item in items]))


def list_expis(ticker=None):
    if ticker:
        items = te_table.query_index("env-ticker-index", "env", param.ENV_USED, True, "ticker", ticker)
    else:
        items = te_table.query_index("env-ticker-index", "env", param.ENV_USED)

    return sorted(set([item["expi"] for item in items]))


def list_dates():
    return sorted([item["trade_date"] for item in state_refs_table.query("env", param.ENV_USED)])
