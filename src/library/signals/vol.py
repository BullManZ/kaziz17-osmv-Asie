import numbers

import pandas as pd


def interp_vol(df, vol_column="iv", linear=True):
    strikes = list(df.strike.values)
    vols = list(df[vol_column].values)
    spot = df.spot.iloc[0]
    try:

        upper = min([k for k in strikes if k >= spot])
        lower = max([k for k in strikes if k < spot])

        up_idx = strikes.index(upper)
        low_idx = strikes.index(lower)
    except:
        return None

    up_vol = vols[up_idx]
    low_vol = vols[low_idx]

    if linear:
        output = low_vol + (up_vol - low_vol) / (upper - lower) * (spot - lower)

    return output


def expi_columns(df, expis):
    for i, expi in enumerate(expis):
        df[expi] = df[0].apply(lambda x: x[i])
    df.drop(columns=[0], inplace=True)
    return df


def num_or_none(l):
    out = None
    for el in l:
        if isinstance(el, numbers.Number):
            out = el
            break
    return out


def atm_vols(df, expis):
    g = df.groupby(["ticker", "expi"])
    iv = g.apply(interp_vol).reset_index().rename(columns={0: "atm_vol"})
    for expi in expis:
        #         df[expi] = "None"
        iv[expi] = iv.apply(lambda row: row["atm_vol"] if (row.expi == expi) else "", axis=1)
    iv.drop(["atm_vol", "expi"], axis=1, inplace=True)

    return iv


def merge_expis(df, cols):
    return [num_or_none(df[col]) for col in cols]


def populate_expis(df, date):
    expis = list(df.expi.unique())
    df_atm_vols = atm_vols(df, expis)
    df_atm_vols["trade_date"] = date
    df_list = df_atm_vols.groupby(["ticker"]).apply(merge_expis, expis).reset_index()

    df_spots = df.groupby(["ticker"]).first().reset_index()[["spot", "trade_date"]]
    df_expis = expi_columns(df_list, expis)

    return pd.concat((df_spots, df_expis), axis=1)
