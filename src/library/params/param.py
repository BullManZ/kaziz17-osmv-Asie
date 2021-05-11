ENV_USED = "test"
IS_LOCAL = True
BUCKET_NAME = "osmv-2"

exclude_list= ["HYG"]
bt_start_date = "2013-01-02"

db_dict = {
    "data_table":"data"+"_"+ENV_USED,
    "vols_table":"vols"+"_"+ENV_USED,
    "errors_table":"errors"+"_"+ENV_USED,
    "rviv_table":"rviv"+"_"+ENV_USED,
    "splits_table":"splits"+"_"+ENV_USED,
    "state_refs_table":"state_refs"+"_"+ENV_USED,
    "ticker_expis_table":"ticker_expis"+"_"+ENV_USED,
    "rv_table":"rvs",
    "iv_table":"ivs"

}

db_schema = {
    "data_table":[db_dict["data_table"],"ref","S",True,"trade_date","S"],
    "vols_table":[db_dict["vols_table"],"ticker","S",True,"trade_date","S"],
    "errors_table":[db_dict["errors_table"],"trade_date","S",False,None,None],
    "rviv_table":[db_dict["rviv_table"],"ticker","S",True,"trade_date","S"],
    "splits_table":[db_dict["splits_table"],"ticker","S",True,"trade_date","S"],
    "state_refs_table":[db_dict["state_refs_table"],"env","S",True,"trade_date","S"],
    "ticker_expis_table":[db_dict["ticker_expis_table"],"env","S",True,"ticker-expi","S"]
}

