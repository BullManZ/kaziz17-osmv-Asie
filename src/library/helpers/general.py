from decimal import Decimal as Dec


def transform_list_based_on_dict(l, d):
    return [transform_el_based_on_dict(el, d) for el in l]


def transform_el_based_on_dict(el, d):
    if el in d.keys():
        return d[el]
    else:
        return el


def month_diff(d1, d2):
    return 12 * (d1.year - d2.year) + (d1.month - d2.month)


def cross_list(list1, list2):
    return {"card 1": len(set(list1)), "card 2": len(set(list2)), "inter": len((set(list1) & set(list2)))}


def null(v):
    if isinstance(v, float):
        if (v != v):
            return True
        else:
            return False
    elif isinstance(v, str):
        return False
    elif v is None:
        return True
    else:
        return False


def scalar_to_decimal(v):
    if (type(v) != str) & (type(v) != bool):
        # output = Dec(int(v * 10000)) / 10000
        output = str(v)
    else:
        output = v
    return output


def decimal_to_scalar(v):
    if type(v) == Dec:
        output = float(v)
    else:
        output = v
    return output


def dict_float_to_decimal(d):
    output = {}
    for k, v in d.items():
        output[k] = scalar_to_decimal(v)

    return output


def df_float_to_decimal(df):
    return df.applymap(scalar_to_decimal)


def df_decimal_to_float(df):
    return df.applymap(decimal_to_scalar)


def bring_columns_first(df, cols):
    all_cols = list(df)
    # move the column to head of list using index, pop and insert
    for c in reversed(cols):
        if c in all_cols:
            all_cols.insert(0, all_cols.pop(all_cols.index(c)))
    return df[all_cols]
# def rescue_code(function):
#     import inspect
#     get_ipython().set_next_input("".join(inspect.getsourcelines(function)[0]))

# os.getcwd()
# os.chdir("/home/ubuntu/omsv/omsv")
