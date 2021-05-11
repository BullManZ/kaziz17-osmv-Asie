#API_KEY = "fZK4tynxFuRWDFGGLs99"
#API_URL_DATES = "https://www.quandl.com/api/v3/datatables/OR/OSMVDATES.csv?&api_key=" + API_KEY

filtering_dict =  {
    #((x > delta_up_1) & (x < delta_up_2)) | ((x > delta_down_2) & (x < delta_down_1)))
    "delta_down_1": 0.5,            #0.5
    "delta_down_2": 0.1,            #0.1
    "delta_up_1": 0.5,              #0.5
    "delta_up_2": 0.9,              #0.9
    "matu_max": 2,
    "matu_min": 0,
    "min_oi_contract": -1,          #50
    "min_oi_name": -1,              #5000
    "min_options_name": 10,         #100
    "min_vol_bo": 0.02,
    "min_volume_contract": -1,
    "min_volume_name": 10           #1000
}

cols = ['ticker', 'stkPx', 'expirDate', 'yte', 'strike', 'cVolu', 'cOi', 'pVolu', 'pOi', 'cBidPx', 'cAskPx',
        'pBidPx', 'pAskPx', 'cBidIv', 'cMidIv', 'cAskIv', 'pBidIv', 'pMidIv', 'pAskIv',
        'delta', 'gamma', 'theta', 'vega', 'rho',
        'spot_px', 'trade_date']

string_cols = ['ticker']
int_cols = ['cVolu', 'cOi', 'pVolu', 'pOi']
date_cols = ['expirDate', 'trade_date']
float_cols = list(set(cols) - set(string_cols) - set(int_cols) - set(date_cols))

DTYPE_DICT = {}
DTYPE_DICT['ticker'] = 'string'
for c in int_cols:
    DTYPE_DICT[c] = 'int'
for c in float_cols:
    DTYPE_DICT[c] = 'float64'

DROP_LIST = ["index", "forward_price", "exchange", "low", "high", "style", "company_name", "open", "close", "isinterpolated", "settlement", "mean_price"]

RENAME_DICT = {'date': 'trade_date','symbol': 'ticker', 'stock_price_close': 'spot', 'expirDate': 'expi','c_volume': 'c_volu', 'p_volume': 'p_volu', 'volume': 'total_volu',  'c_open_interest': 'c_oi','p_open_interest': 'p_oi','total_open_interest': 'total_oi', 'cBidPx': 'c_bid','cAskPx': 'c_ask','pBidPx': 'p_bid','pAskPx': 'p_ask','cBidIv': 'c_iv_bid','cMidIv': 'c_iv_mid','cAskIv': 'c_iv_ask','pBidIv': 'p_iv_bid','pMidIv': 'p_iv_mid','pAskIv': 'p_iv_ask',}
