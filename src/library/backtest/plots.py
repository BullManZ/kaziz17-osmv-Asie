import plotly.graph_objects as go
from plotly.subplots import make_subplots

def plot_col(df, col):
    fig = go.Figure()

    fig = make_subplots()

    fig.add_trace(go.Scatter(x=df.index, y=df[col],
                             mode='lines+markers',
                             name=col), secondary_y=False)

    fig.update_traces(marker=dict(size=5))
    return fig


def plot_cols(df, cols):
    fig = go.Figure()

    fig = make_subplots()
    for col in cols:
        fig.add_trace(go.Scatter(x=df.index, y=df[col],
                                 mode='lines+markers',
                                 name=col), secondary_y=False)

    fig.update_traces(marker=dict(size=5))
    return fig


def plot_backtest(df, pnl_cols, other_cols):
    figs = {}
    for col in other_cols:
        figs[col] = plot_col(df, col)

    pnl_fig = plot_cols(df, pnl_cols)
    return (pnl_fig, figs)