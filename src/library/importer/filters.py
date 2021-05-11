from src.library.helpers.dates import is_quarterly_expi
from src.library.helpers.dates import is_third_friday


def filter_new_refs(date, df, filtering_dict, current_df):
    columns = ["ticker"]
    date = str(date)
    if is_quarterly_expi(date):

        filt_df = filter_tickers(df, filtering_dict)
        df = add_new_to_current(df, filt_df, columns, current_df)
    else:
        df = keep_current(df, columns, current_df)

    columns = ["ticker", "expi"]
    if is_third_friday(date):
        filt_df = filter_ticker_expis(df, filtering_dict)
        df = add_new_to_current(df, filt_df, columns, current_df)
    else:
        df = keep_current(df, columns, current_df)

    columns = ["ref"]

    filt_df = filter_refs(df, filtering_dict)

    df = add_new_to_current(df, filt_df, columns, current_df)

    find_stock_splits(current_df, df)

    return df


def filter_first_refs(df, filtering_dict):
    df = filter_tickers(df, filtering_dict)
    df = filter_ticker_expis(df, filtering_dict)
    df = filter_refs(df, filtering_dict)
    return df


def find_stock_splits(df1, df2):
    return True


def add_new_to_current(df, filt_df, columns, current_df):
    if len(columns) == 1:
        df["comp"] = df[columns[0]]
        filt_df["comp"] = filt_df[columns[0]]
        current_df["comp"] = current_df[columns[0]]
    else:
        df["comp"] = df[columns].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)
        filt_df["comp"] = filt_df[columns].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)
        current_df["comp"] = current_df[columns].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)

    combined = set(current_df["comp"].unique()).union(set(filt_df["comp"].unique()))

    df = df[df.comp.isin(combined)]
    df.drop(["comp"], axis=1, inplace=True)

    return df


def keep_current(df, columns, current_df):
    if len(columns) == 1:
        df["comp"] = df[columns[0]]
        current_df["comp"] = current_df[columns[0]]
    else:
        df["comp"] = df[columns].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)
        current_df["comp"] = current_df[columns].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)

    combined = set(current_df["comp"].unique())
    df = df[df.comp.isin(combined)]
    df.drop(["comp"], axis=1, inplace=True)

    return df


def filter_min_options(df, min_options_name):
    return df.groupby(["ticker"]).filter(lambda x: len(x) > min_options_name)


def filter_delta(df, delta_up_1, delta_up_2, delta_down_2, delta_down_1):
    return df[df["delta"].apply(
        lambda x: (((x > delta_up_1) & (x < delta_up_2)) | ((x > delta_down_2) & (x < delta_down_1))))]


def filter_yte(df, matu_min, matu_max):
    return df[(df.yte < matu_max) & (df.yte > matu_min)]


def filter_oi(df, min_oi_name):
    return df.groupby(["ticker", "expi"]).filter(lambda x: x.total_oi.sum() > min_oi_name)


def filter_volume(df, min_volume_name):
    grouped = df.groupby(["ticker", "expi"])
    df_grouped = grouped.filter(lambda x: x.total_volu.sum() > min_volume_name)
    return df_grouped


def filter_contracts(df, min_oi_contract, min_volume_contract):
    return df[(df["total_oi"] > min_oi_contract) & (df["total_volu"] > min_volume_contract)]


def filter_min_vol_bo(df, min_vol_bo):
    return df[(df["min_ba"] < min_vol_bo)]


def filter_tickers(df, filtering_dict):
    output = filter_min_options(df, filtering_dict["min_options_name"])
    # output = df[df.ticker == "AAPL"]
    return output


def filter_ticker_expis(df, filtering_dict):
    output = filter_yte(df, filtering_dict["matu_min"], filtering_dict["matu_max"])
    output = filter_oi(output, filtering_dict["min_oi_name"])

    output = filter_volume(output, filtering_dict["min_volume_name"])

    return output


def filter_refs(df, filtering_dict):
    output = filter_delta(df, filtering_dict["delta_up_1"], filtering_dict["delta_up_2"],
                          filtering_dict["delta_down_2"], filtering_dict["delta_down_1"])
    output = filter_contracts(output, filtering_dict["min_oi_contract"], filtering_dict["min_volume_contract"])
    output = filter_min_vol_bo(output, filtering_dict["min_vol_bo"])

    return output
