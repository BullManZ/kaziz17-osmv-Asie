import datetime
import re
import time

import boto3
import requests

from src.library.osmv.Osmv import Osmv
from src.library.params import param
from src.library.params.param import BUCKET_NAME
import src.library.params.import_param as import_param

s3c = boto3.client('s3')

# def get_dates():
#     res = requests.get(import_param.API_URL_DATES)
#     list_of_dates = res.text.split('\n')[1:]
#     list_obj_dates = [re.sub('-', '', line) for line in list_of_dates]
#     return list_of_dates, list_obj_dates

list_obj_dates = []
list_of_dates = []

osmv = Osmv(param.IS_LOCAL, param.BUCKET_NAME)
(dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.select_env(param.ENV_USED)

# paginator = s3c.get_paginator('list_objects_v2')
# pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix='prefix')
# for page in pages:
#     for obj in bucket.objects.all():
#         list_obj_dates.append(obj.key.encode('utf-8')[-8:])
#
# for elt in list_obj_dates:
#     tmp = (list_obj_dates[elt], "%Y%m%d").date()
#     list_of_dates.append(tmp)
def get_dates():
    list_obj_dates = []
    list_of_dates = []

    # paginator = s3c.get_paginator('list_objects_v2')
    # pages = paginator.paginate(Bucket='osmv-2', Prefix='prefix')
    # for page in pages:
    for obj in bucket.objects.all():
        tmp = obj.key[-12:-4] #+ obj.key[-8:-6] + obj.key[-6:-4]
        if obj.key.endswith(".csv"):
            list_obj_dates.append(tmp)

    for elt in range(len(list_obj_dates)):
        var = list_obj_dates[elt]
        tmp = datetime.datetime.strptime(var, '%Y%m%d').date()
        list_of_dates.append(tmp)
    return list_of_dates, list_obj_dates


#def backtest_rebal_dates_monthly(list_of_dates):
#    return [d for d in list_of_dates[:-1] if is_third_friday(d)]

def backtest_rebal_dates_monthly(list_of_dates):
    return [d for d in list_of_dates[:-1] if is_third_friday(d)]


def backtest_rebal_dates_weekly(list_of_dates):
    return [d for d in list_of_dates[:-1] if (weekday(d) == 4)]


def backtest_rebal_dates_quarterly(list_of_dates):
    return [d for d in list_of_dates[:-1] if is_quarterly_expi(d)]


def rebal_fixed_strike(list_of_dates):
    return [d for d in list_of_dates[:-1] if (weekday(d) == 4)]


def rebal_dates_array(list_of_dates):
    return [d for d in list_of_dates[:-1] if (weekday(d) == 4)]


def pluck_date_index_list(start, end, dates_array, frequency="Daily"):
    start_index = dates_array.index(start)
    end_index = dates_array.index(end)
    if frequency == "Daily":
        return dates_array[start_index:(end_index + 1)]


def validate(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")


def weekday(date_text):
    validate(date_text)
    return datetime.datetime.strptime(date_text, '%Y-%m-%d').weekday()


def find_first_date_after(date_text, dates_array):
    validate(date_text)
    if date_text in dates_array:
        return date_text
    else:
        return min([d for d in dates_array if d >= date_text])


def find_last_date_before(date_text, dates_array):
    validate(date_text)
    if date_text in dates_array:
        return date_text
    else:
        return max([d for d in dates_array if d <= date_text])


def add_days(date_string, num_days):
    date_1 = datetime.datetime.strptime(date_string, '%Y-%m-%d')
    end_date = date_1 + datetime.timedelta(days=num_days)
    return end_date.strftime('%Y-%m-%d')


def is_third_friday(s):
    d = datetime.datetime.strptime(s, '%Y-%m-%d')
    return d.weekday() == 4 and 15 <= d.day <= 21


def is_quarterly(s):
    d = datetime.datetime.strptime(s, '%Y-%m-%d')
    return (d.month % 3) == 0


def is_quarterly_expi(s):
    return (is_quarterly(s) and is_third_friday(s))


def next_date(date, array):
    try:
        index = array.index(date)
        return array[index + 1]

    except:
        raise ValueError("Date Error")


def prev_date(date, array):
    try:
        index = array.index(date)
        return array[index - 1]

    except:
        raise ValueError("Date Error")


def date_or_next(date, array):
    validate(date)
    if date in array:
        return date
    else:
        return min([d for d in array if (d >= date)])


def date_intervals(start_date, end_date, rebal_dates):
    (list_of_dates, list_obj_dates) = get_dates()
    output = [date_or_next(start_date, list_of_dates)]
    [output.append(d) for d in rebal_dates if ((d > start_date) & (d < end_date))]
    output.append(find_last_date_before(end_date, list_of_dates))
    return output

