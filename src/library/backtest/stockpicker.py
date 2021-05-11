import pandas as pd

from src.library.dynamo.Table import Table
from src.library.osmv.Osmv import Osmv
from src.library.params import param

osmv = Osmv(param.IS_LOCAL, param.BUCKET_NAME)
(dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.select_env(param.ENV_USED)


def select_signal_sequence(date,  table_name, list_signals, list_num_stocks, list_largest, excluding_list):
    signals = pd.DataFrame(Table(dbr.Table(table_name)).query_index("reverse","trade_date",date))
    if len(signals) > 0:
        for i, s in enumerate(list_signals):
            signals = signals[~signals.ticker.isin(excluding_list)]
            signals = signals.sort_values(by=s, ascending=~list_largest[i]).head(list_num_stocks[i])

        return list(signals.ticker)
    else:
        return []

# def select_signal_ranks(date, signal_dfs, num_stocks, list_signals, list_lambdas, list_largest):
#     rank_dfs = [signal_dfs[s].loc[date].rank(na_option="top", ascending=list_largest[i]) for i, s in
#                 enumerate(list_signals)]
#     weighted_ranks = sum([list_lambdas[i] * rank_dfs[i] for i in range(len(rank_dfs))])
#     return list(weighted_ranks.nlargest(num_stocks).index)
