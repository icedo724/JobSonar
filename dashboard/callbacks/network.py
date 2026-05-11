"""스킬 네트워크 탭 콜백 (공동 출현 그래프)."""
import plotly.graph_objects as go
from dash import Input, Output, html

from analysis import build_cooccurrence_graph, graph_to_plotly_traces
from db.connection import get_conn
from dashboard.context import WHITE
from dashboard.utils import empty_fig


def register(app) -> None:

    @app.callback(
        Output("network-graph", "figure"),
        Output("network-metrics", "children"),
        Input("net-category", "value"),
        Input("net-min-cooccur", "value"),
    )
    def update_network(net_cat, min_cooccur):
        with get_conn() as conn:
            G = build_cooccurrence_graph(
                conn, category=net_cat, min_cooccur=min_cooccur or 3
            )

        if len(G.nodes) == 0:
            return empty_fig("조건에 맞는 연결 없음 — 최소 출현 횟수를 낮춰보세요"), []

        avg_degree = sum(d for _, d in G.degree()) / len(G.nodes)
        metrics = [
            html.Div([
                html.P("스킬 노드", className="kpi-label"),
                html.P(str(len(G.nodes)), className="kpi-value"),
            ], className="kpi-card"),
            html.Div([
                html.P("연결 엣지", className="kpi-label"),
                html.P(str(len(G.edges)), className="kpi-value"),
            ], className="kpi-card"),
            html.Div([
                html.P("평균 연결 수", className="kpi-label"),
                html.P(f"{avg_degree:.1f}", className="kpi-value"),
            ], className="kpi-card"),
        ]

        edge_traces, node_traces = graph_to_plotly_traces(G)
        fig = go.Figure(
            data=edge_traces + node_traces,
            layout=go.Layout(
                showlegend=False, hovermode="closest", height=560,
                plot_bgcolor=WHITE, paper_bgcolor=WHITE,
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            ),
        )
        return fig, metrics
