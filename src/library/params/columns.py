
pnl_cols = [
                 'spot_move',
                 'c_mid_move',
                 'p_mid_move',
                 'yte_move',
                 'c_iv_mid_move',
                 'p_iv_mid_move',
                 's_mid_move',
                 'c_delta_pnl',
                 'p_delta_pnl',
                 'theta_pnl',
                 'gamma_pnl',
                 'rho_pnl',
                 'c_pnl_explain',
                 'p_pnl_explain',
                 'cdh_pnl',
                 'pdh_pnl',
                 'c_vol_pnl_mid',
                 'p_vol_pnl_mid',
                 'otm_iv_mid_move',
                 'c_imp_vol_move',
                 'p_imp_vol_move',
                 ]

#RHO_pnl=0
gen_cols = [
    'trade_date',
    'ref',
    'ticker',
    'spot',
    'expi',
    'yte',
    'strike',
    'strike_%'
]
greek_cols = [
    'delta',
    'gamma',
    'theta',
    'vega',
    'rho'
]

liqu_cols = [
    'c_volu',
    'c_oi',
    'p_volu',
    'p_oi',
    'total_oi',
    'total_volu',
    'c_ba',
    'p_ba',
    'min_ba',
]
call_cols = [
    'c_bid',
    'c_mid',
    'c_ask',
]
put_cols = [
    'p_bid',
    'p_ask',
    'p_mid',
]
vol_cols = [
    'iv',
    'c_iv_bid',
    'c_iv_mid',
    'c_iv_ask',
    'p_iv_bid',
    'p_iv_mid',
    'p_iv_ask',
]
other_cols = [
    's_ask',
    's_bid',
]

all_cols = gen_cols + greek_cols + liqu_cols + call_cols + put_cols + vol_cols + other_cols
