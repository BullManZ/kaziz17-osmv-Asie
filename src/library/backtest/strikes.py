from operator import itemgetter


distance_K = 0.05
alpha_K = 1


def select_new_strikes(spot, ref_strikes):
    perc_strikes = [k / spot for k in ref_strikes]

    min_K = distance_K * (int(min(perc_strikes) / distance_K))

    max_K = distance_K * int(max(perc_strikes) / distance_K)

    amp_K = max_K - min_K

    num_K = round(amp_K / distance_K)

    if num_K >= 1:

        return [round(min_K + i * distance_K, 2) for i in range(1, num_K)]
    else:
        raise ValueError("Not enough strikes")


def select_listed_strike(strike, ref_strikes):
    distances = [abs(K - strike) for K in ref_strikes]
    index, element = min(enumerate(distances), key=itemgetter(1))
    return ref_strikes[index]


def select_from_listed_strikes(df_date, strikes):
    # no liquidity considerations for now
    available_strikes = sorted([(x) for x in (df_date.strike)], reverse=True)

    spot = df_date.spot.iloc[0]
    theo_strikes = [spot * K for K in strikes]

    output = []
    for K in theo_strikes:
        output.append(select_listed_strike(K, available_strikes))

    return output


def select_refs_for_strikes(df, strikes=None):
    spot = (df.spot.iloc[0])

    if strikes == None:
        ref_strikes = [(x) for x in (df.strike)]
        strikes = select_new_strikes(spot, ref_strikes)

    ref_strikes = select_from_listed_strikes(df, strikes)

    refs = [df[df.strike == strike].ref.iloc[0] for strike in ref_strikes]
    output = dict(zip(strikes, refs))
    #     print(output)
    return output
