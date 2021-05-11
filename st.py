import base64
from io import BytesIO

import pandas as pd
import streamlit as st

from src.library.backtest.backtester import backtest_bo
from src.library.backtest.greekneutral import greek_neutral_ts
from src.library.dynamo.Client import Client
from src.library.dynamo.Table import Table
from src.library.helpers.dates import get_dates
from src.library.helpers.lists import list_tickers
from src.library.osmv.Osmv import Osmv
from src.library.params import param

osmv = Osmv(param.IS_LOCAL, param.BUCKET_NAME)
(dbr, dbc, s3r, s3c, bucket, db_dict) = osmv.select_env(param.ENV_USED)
(list_of_dates, list_obj_dates) = get_dates()

DB = Client(dbc)
data_table = Table(dbr.Table(db_dict["data_table"]))
vols_table = Table(dbr.Table(db_dict["vols_table"]))
rv_table = Table(dbr.Table(db_dict["rv_table"]))
iv_table = Table(dbr.Table(db_dict["iv_table"]))

from src.library.helpers.lists import list_dates
from src.library.helpers.lists import list_expis

from src.library.backtest.plots import plot_backtest
from src.library.backtest.singlename import base_bo_ts, ticker_bo_ts
from src.library.helpers.general import bring_columns_first
from src.library.helpers.general import transform_list_based_on_dict

from src.library.params import rviv as param_rviv
from src.library.signals.calc import select_signals

from src.library.signals.iv import query_vols
from src.library.signals.rviv import query_rviv


def main():
    # Render the readme as markdown using st.markdown.
    st.set_page_config(page_title='OSMV', layout='wide', initial_sidebar_state='auto')
    readme_text = (st.title('Welcome to OSMV v.3 👋'), st.markdown('👈 Select app mode on the sidebar'))

    # Once we have the dependencies, add a selector for the app mode on the sidebar.
    st.sidebar.title("What to do")
    modes = ["Show instructions", "Show the source code", "Spots and vols", "Signals", "Fixed Expi Time Serie",
             "Rolling Expi Time Serie", "Greek Neutral Time Serie", "Backtest"]
    #     ,"Test"]
    app_mode = st.sidebar.selectbox("Choose the app mode", modes)
    if app_mode == modes[0]:
        st.sidebar.success('To continue select an option.')
    elif app_mode == modes[1]:
        readme_text[0].empty()
        readme_text[1].empty()
        st.code(get_file_content_as_string("st.py"))
    elif app_mode == modes[2]:
        readme_text[0].empty()
        readme_text[1].empty()
        show_spots()
    elif app_mode == modes[3]:
        readme_text[0].empty()
        readme_text[1].empty()
        show_signals()
    elif app_mode == modes[4]:
        readme_text[0].empty()
        readme_text[1].empty()
        base_time_serie()
    elif app_mode == modes[5]:
        readme_text[0].empty()
        readme_text[1].empty()
        ticker_time_serie()
    elif app_mode == modes[6]:
        readme_text[0].empty()
        readme_text[1].empty()
        greek_neutral()
    elif app_mode == modes[7]:
        readme_text[0].empty()
        readme_text[1].empty()
        backtest_sequential_signal()
    elif app_mode == modes[8]:
        readme_text[0].empty()
        readme_text[1].empty()
        test()


def get_file_content_as_string(file_name):
    with open(file_name, 'r') as file:
        data = file.read()
        return data


def get_table_download_link(df, st_name, file_name):
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
    href = f'<a href="data:file/csv;base64,{b64}" download="' + file_name + '.csv">Download ' + st_name + '</a>'

    return href


def to_excel(dict_of_df):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    workbook = writer.book
    for df_name, df_meta in dict_of_df.items():
        df_meta["df"].to_excel(writer, sheet_name=df_name, index=False, startrow=10)
        worksheet = writer.sheets[df_name]
        meta = df_meta["meta"]

        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#D7E4BC',
            'border': 1})

        for num, key in enumerate(df_meta["meta"].keys()):
            worksheet.write(num, 0, key, header_format)
            worksheet.write(num, 1, meta[key], header_format)

    writer.save()
    processed_data = output.getvalue()
    return processed_data


def test():
    download = st.button('Test')
    if download:
        dict_of_df = {}
        dict_of_df["abc"] = pd.DataFrame(
            {"df": pd.DataFrame({"df": [1, 2], "b": [2, 3]}), "meta": {"k1": "v1", "k2": "v2"}})
        dict_of_df["def"] = pd.DataFrame(
            {"df": pd.DataFrame({"df": [1, 2], "b": [2, 3]}), "meta": {"k1": "v1", "k2": "v2"}})

        st.markdown(excel_download_link(dict_of_df, "xx", "xx.xlsx"), unsafe_allow_html=True)


def excel_download_link(dict_of_df, link_name, file_name):
    val = to_excel(dict_of_df)
    b64 = base64.b64encode(val)
    return f'<a href="data:application/octet-stream;base64,{b64.decode()}" download="' + file_name + '">' + link_name + '</a>'  # decode b'abc' => abc


# def init_signals():
#     maturity_suffix_list = ["3m", "6m", "1y"]
#     pct_prefix_list = param_rviv.pct_prefix_list
#     median_prefix_list = param_rviv.median_prefix_list
#     proba_1_prefix_list = param_rviv.proba_1_prefix_list
#
#     maturity = [st.sidebar.selectbox("Choose maturity", maturity_suffix_list)]
#     pct = st.sidebar.multiselect("Choose percentile signals", pct_prefix_list, default=pct_prefix_list)
#     median = st.sidebar.multiselect("Choose median signals", median_prefix_list, default=median_prefix_list)
#     proba = st.sidebar.multiselect("Choose proba higher than 100% signals", proba_1_prefix_list,
#                                    default=proba_1_prefix_list)
#
#     return select_signals(maturity, pct, median, proba)


def show_spots():
    def export_spots_vols(tickers):
        for t in tickers:
            df_raw = bring_columns_first(pd.DataFrame(query_vols(t)), ["trade_date", "ticker", "spot"])
            df_rviv = (query_rviv(t))
            st.title("Raw data")
            st.write(df_raw.head())
            st.title("RV IV")
            st.write(df_rviv.head())

            st.markdown(get_table_download_link(df_raw, "raw_data_" + t, "raw_data_" + t), unsafe_allow_html=True)
            st.markdown(get_table_download_link(df_rviv, "rv_iv_" + t, "rv_iv_" + t), unsafe_allow_html=True)

    tickers = st.sidebar.multiselect("Select tickers", list_tickers())
    export_spots_vols(tickers)


def show_signals():
    list_tables = DB.list_tables()
    signals_tables = [t for t in list_tables if t[:7] == "signals"]
    selected_signal = st.sidebar.selectbox("Select signals", signals_tables)
    date = st.sidebar.selectbox("Choose start date", list_dates())
    run_btn = st.sidebar.button('Show signals')
    if run_btn:
        df = pd.DataFrame(Table(dbr.Table(selected_signal)).query_index("reverse", "trade_date", date))
        df = bring_columns_first(df, ["spot"]).set_index("ticker")
        st.write(df)
        st.markdown(get_table_download_link(df, selected_signal, selected_signal), unsafe_allow_html=True)


def base_time_serie():
    st.sidebar.title("Select base time serie params")

    ticker = st.sidebar.selectbox("Select ticker", list_tickers())
    expi = st.sidebar.selectbox("Select expiry", list_expis(ticker))

    st.sidebar.write('Now on to time serie params 👉')

    st.title("Time Serie parameters")

    start = st.selectbox("Choose start date", list_dates())
    end = st.selectbox("Choose end date", [d for d in list_dates() if (d > start)])

    strike = st.selectbox("Choose strike", [1, 0.9, 0.95, 1.05, 1.1])
    threshold = st.selectbox("Choose stock time serie threshold", [0.03, 0.01, 0.02])

    export_all = st.checkbox('Export all the data to excel')

    bt_btn = st.button('Run calculation')

    if bt_btn:
        (dict_df, exports) = base_bo_ts(start, end, ticker, expi, [strike], threshold, export_all)
        df = dict_df[strike]
        st.title("Time Serie Data")

        if export_all:
            st.markdown(excel_download_link(exports, "Download complete fixed expi data",
                                            "fixed_expi_ts" + '_' + ticker + '_' + expi + ".xlsx"),
                        unsafe_allow_html=True)
        else:
            st.markdown(get_table_download_link(df, "fixed_expi_ts" + '_' + ticker + '_' + expi,
                                                "fixed_expi_ts" + '_' + ticker + '_' + expi), unsafe_allow_html=True)

        st.write(df)


def ticker_time_serie():
    st.sidebar.title("Select ticker base time serie params")

    ticker = st.sidebar.selectbox("Select ticker", list_tickers())

    st.sidebar.write('Now on to time serie params 👉')

    st.title("Time Serie parameters")

    maturity_string = st.selectbox("Choose maturity", ["3m", "6m", "1y"])

    start = st.selectbox("Choose start date", list_dates())
    end = st.selectbox("Choose end date", [d for d in list_dates() if (d > start)])

    strike = st.selectbox("Choose strike", [1, 0.9, 0.95, 1.05, 1.1])
    threshold = st.selectbox("Choose stock time serie threshold", [0.03, 0.01, 0.02])

    export_all = st.checkbox('Export all the data to excel')

    bt_btn = st.button('Run calculation')

    if bt_btn:
        (dd, exports) = ticker_bo_ts(ticker, [strike], maturity_string, start, end, threshold, export_all)
        df = dd[strike]
        st.title("Time Serie Data")

        if export_all:
            st.markdown(excel_download_link(exports, "Download complete time serie data",
                                            "roll_expi_ts" + '_' + ticker + ".xlsx"), unsafe_allow_html=True)
        else:
            st.markdown(get_table_download_link(df, "roll_expi_ts" + '_' + ticker, "roll_expi_ts" + '_' + ticker),
                        unsafe_allow_html=True)

        st.write(df)


@st.cache
def list_stocks():
    return list_tickers()


def greek_neutral():
    st.sidebar.title("Select stocks")

    stocks_long = st.sidebar.multiselect("Select longs", list_stocks())
    stocks_short = st.sidebar.multiselect("Select shorts", list_stocks())
    alpha_strike_cost = st.sidebar.slider("Choose alpha for strike roll cost (0=mid, 1=100%*(bid/ask to mid)", 0.0, 1.0, 0.5)
    alpha_expi_cost = st.sidebar.slider("Choose alpha for expi roll cost (0=mid, 1=100%*(bid/ask to mid)", 0.0, 1.0, 0.5)

    st.sidebar.write('Now on to time serie params 👉')

    st.title("Time Serie parameters")

    maturity_string = st.selectbox("Choose maturity", ["3m", "6m", "1y"])

    start = st.selectbox("Choose start date", list_dates())
    end = st.selectbox("Choose end date", [d for d in list_dates() if (d > start)])

    greek = st.selectbox("Choose greek", ["theta", "vega", "gamma"])
    strike = st.selectbox("Choose strike", [1, 0.9, 0.95, 1.05, 1.1])
    export_all = st.checkbox('Export all the data to excel')

    cols_to_plot = st.multiselect("Choose columns to plot",
                                  ["Day PNL", "Gross PNL", "Net PNL", "theta", "vega", "gamma"], default=["Net PNL"])
    dict_cols_to_plot = {"Day PNL": "cdh_pnl", "Gross PNL": "gross_pnl", "Net PNL": "net_pnl"}
    code_cols_to_plot = transform_list_based_on_dict(cols_to_plot, dict_cols_to_plot)

    bt_btn = st.button('Run calculation')

    if bt_btn:
        ls_df, start_risk_dict, end_risk_dict, longs, shorts, exports = greek_neutral_ts(greek,
                                                                                strike,
                                                                                start,
                                                                                end,
                                                                                maturity_string,
                                                                                stocks_long,
                                                                                stocks_short, export_all)

        ls_df["total_cost"] = alpha_strike_cost * ls_df["strike_roll_cost"] + alpha_expi_cost * ls_df["expi_roll_cost"]
        ls_df["day_net_pnl"] = ls_df["cdh_pnl"] - ls_df["total_cost"]
        ls_df["gross_pnl"] = ls_df["cdh_pnl"].cumsum()
        ls_df["net_pnl"] = ls_df["day_net_pnl"].cumsum()
        ls_df.iloc[0]["gross_pnl"] = 0
        ls_df.iloc[0]["net_pnl"] = 0

        greek_ts_output(ls_df, code_cols_to_plot, alpha_strike_cost, alpha_expi_cost, export_all, exports)


def greek_ts_output(df, code_cols_to_plot, alpha_strike_cost, alpha_expi_cost, export_all, exports):
    st.title("Greek neutral time serie Data")
    if export_all:
        st.markdown(excel_download_link(exports, "Download complete GN TS data", "GN.xlsx"),
                    unsafe_allow_html=True)
    else:
        st.markdown(get_table_download_link(df.reset_index().rename(columns={"index": "trade_date"}), "GN", "GN"), unsafe_allow_html=True)

    st.write(df)

    pnl_cols = [c for c in code_cols_to_plot if c in ["net_pnl", "gross_pnl"]]
    other_cols = [c for c in code_cols_to_plot if c not in ["net_pnl", "gross_pnl"]]
    (pnl_fig, figs) = plot_backtest(df, pnl_cols, other_cols)

    st.title("PNL")
    st.plotly_chart(pnl_fig)

    for col, fig in figs.items():
        st.title(col)
        st.plotly_chart(fig)


def write_sentences(num_stocks, orders, signals_used, rebal_dates_freq):
    sentences = []
    for i, n in enumerate(num_stocks):
        if orders[i]:
            order_word = "largest"
        else:
            order_word = "lowest"
        if i == 0:
            sentences.append(
                "First, we select the " + str(n) + " stocks with the " + order_word + " " + signals_used[i] + ".")
        else:
            sentences.append(
                "Then, out of those we select the " + str(n) + " stocks with the " + order_word + " " + signals_used[
                    i] + ".")

    st.write("Selection of stocks process, done " + rebal_dates_freq + ".")
    for s in sentences:
        st.write(s)


def backtest_sequential_signal():
    st.sidebar.title("Signal parameters")
    maturity_suffix_list = ["3m", "6m", "1y"]

    maturity_string = st.sidebar.selectbox("Choose maturity", maturity_suffix_list)
    selected_signals = select_signals([maturity_string], param_rviv.pct_prefix_list, param_rviv.median_prefix_list,
                                      param_rviv.proba_1_prefix_list)

    all_signal_cols = selected_signals["all"]

    signals_used = st.sidebar.multiselect("Choose signals to use ", all_signal_cols)

    num_stocks = []
    orders = []
    last_num_stock = 200

    for s in signals_used:
        num = st.sidebar.slider("Select number of stocks for " + s, 1, max_value=last_num_stock, value=last_num_stock,
                                step=1)
        order = st.sidebar.checkbox('Select largest for ' + s)
        num_stocks.append(num)
        orders.append(order)
        last_num_stock = num

    rebal_dates_freq = st.sidebar.selectbox("Choose signal calculation frequency", ["weekly", "monthly"])

    st.sidebar.write('Now on to backtest params 👉')

    st.title("Backtest parameters")
    write_sentences(num_stocks, orders, signals_used, rebal_dates_freq)

    start = st.selectbox("Choose start date", [d for d in list_dates() if (d >= param.bt_start_date)])
    end = st.selectbox("Choose end date", [d for d in list_dates() if (d > start)])
    ref_ticker = st.selectbox("Choose ref index", ["SPY"])
    excluding_list = st.sidebar.multiselect("Select stocks to exclude", list_stocks(), default=["HYG"])
    greek = st.selectbox("Choose greek", ["theta", "vega", "gamma"])
    strike = st.selectbox("Choose strike", [0.9, 0.95, 1])
    alpha_strike_cost = st.sidebar.slider("Choose alpha for strike roll cost (0=mid, 1=100%*(bid/ask to mid)", 0.0, 1.0, 0.5)
    alpha_expi_cost = st.sidebar.slider("Choose alpha for expi roll cost (0=mid, 1=100%*(bid/ask to mid)", 0.0, 1.0, 0.5)
    alpha_stocks_roll_cost = st.sidebar.slider("Choose alpha for basket draft cost(0=mid, 1=100%*(bid/ask to mid)", 0.0, 1.0, 0.5)

    dict_cols_to_plot = {"Day gross PNL": "cdh_pnl", "Day net PNL": "day_net_pnl", "Gross PNL": "gross_pnl",
                         "Net PNL": "net_pnl", "theta": "theta", "vega": "vega", "gamma": "gamma"}
    col_keys = list(dict_cols_to_plot.keys())
    cols_to_plot = st.multiselect("Choose columns to plot",
                                  col_keys,
                                  default=[col_keys[2], col_keys[3]])


    code_cols_to_plot = transform_list_based_on_dict(cols_to_plot, dict_cols_to_plot)

    export_all = st.checkbox('Export all the data to excel')
    bt_btn = st.button('Calc signal and run backtest')

    if bt_btn:
        (df, exports) = backtest_sequence_cache(greek, strike, start, end, maturity_string,
                                                signals_used, ref_ticker, num_stocks, orders,
                                                rebal_dates_freq, excluding_list, export_all)

        df["total_cost"] = alpha_strike_cost * df["strike_roll_cost"] + alpha_expi_cost * df[
            "expi_roll_cost"] + alpha_stocks_roll_cost * df["io_cost"]

        df["day_net_pnl"] = df["cdh_pnl"] - df["total_cost"]

        df["gross_pnl"] = df["cdh_pnl"].cumsum()
        df["net_pnl"] = df["day_net_pnl"].cumsum()
        df.iloc[0]["gross_pnl"] = 0
        df.iloc[0]["net_pnl"] = 0

        backtest_output(df, exports, export_all, code_cols_to_plot, alpha_strike_cost, alpha_expi_cost, alpha_stocks_roll_cost)


def backtest_output(df, exports, export_all, code_cols_to_plot, alpha_strike_cost, alpha_expi_cost, alpha_stocks_roll_cost):
    st.title("Backtest Data")
    if export_all:
        st.markdown(excel_download_link(exports, "Download complete backtest data", "backtest.xlsx"),
                    unsafe_allow_html=True)
    else:
        st.markdown(get_table_download_link(df.reset_index().rename(columns={"index": "trade_date"}), "backtest", "backtest"), unsafe_allow_html=True)


    st.write(df)

    pnl_cols = [c for c in code_cols_to_plot if c in ["net_pnl", "gross_pnl"]]
    other_cols = [c for c in code_cols_to_plot if c not in ["net_pnl", "gross_pnl"]]
    (pnl_fig, figs) = plot_backtest(df, pnl_cols, other_cols)

    st.title("PNL")
    st.plotly_chart(pnl_fig)

    for col, fig in figs.items():
        st.title(col)
        st.plotly_chart(fig)


@st.cache(allow_output_mutation=True)
def backtest_sequence_cache(greek, strike, start, end, maturity_string, signals_used, ref_ticker, num_stocks,
                            orders, freqs, excluding_list, export_dfs):
    return backtest_bo(greek, strike, start, end, maturity_string, signals_used, ref_ticker, num_stocks,
                       orders, freqs, excluding_list, export_dfs)


if __name__ == "__main__":
    main()
