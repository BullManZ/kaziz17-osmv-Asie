expi_threshold = 0.15
delta_threshold = 0.15
bidask_cap = 0.05
max_distance_one_expi = 10

strike_change_threshold = 0.03

GREEK_TS_COLS = ["vega", "gamma", "theta", "delta"]
COST_COLS = ["strike_roll_cost", "expi_roll_cost"]
KEEP_COLS = GREEK_TS_COLS + ["cdh_pnl"]

ALPHA_TS_COLS = ["vega", "gamma", "theta", "delta", "cdh_pnl", "strike_roll_cost"]
COMMON_TS_COLS = ["trade_date", "spot", "theo_expi"]
EXPI_TS_COLS = ["expi", "expi_dt", "min_ba"]
BASE_COLS = ['ref',
             'trade_date',
             'ticker',
             'strike',
             'fixed_k',
             'expi',
             'spot',
             'cdh_pnl',
             'pdh_pnl',
             'delta',
             'theta',
             'vega',
             'gamma',
             'p_ba',
             'c_ba',
             'min_ba',
             'spot_move',
             'new_ref',
             'gamma_pnl',
             'theta_pnl',
             'osmv_vol',
             'strike_vega_change',
             ]
