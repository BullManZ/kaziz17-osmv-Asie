import warnings

from pandas.core.common import SettingWithCopyWarning

from src.library.dynamo.Client import Client
from src.library.helpers.dates import get_dates
from src.library.osmv.Osmv import Osmv
from src.library.params import param

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

osmv = Osmv(param.IS_LOCAL,param.BUCKET_NAME)

(dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.create_env("test",param.db_dict,param.db_schema)
Client(dbc).add_index("data_test", "reverse", "trade_date", "S", "ref", "S")

(list_of_dates,list_obj_dates) = get_dates()



