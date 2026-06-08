"""사이드바 메트릭 + KPI 카드 콜백."""
from dash import Input, Output, html

from analysis import new_jobs_count
from dashboard.context import get_jobs_df
from dashboard.utils import apply_filter, kpi_card


def register(app) -> None:

    @app.callback(
        Output("sidebar-metrics", "children"),
        Input("filter-categories", "value"),
        Input("filter-sources", "value"),
        Input("filter-industry", "value"),
        Input("filter-emp-type", "value"),
        Input("data-version", "data"),
    )
    def update_sidebar(categories, sources, industries, emp_types, _version):
        df = apply_filter(get_jobs_df(), categories, sources, industries, emp_types)
        new7 = new_jobs_count(df, days=7)
        last = df["collected_at"].max().strftime("%Y-%m-%d") if not df.empty else "—"
        return [
            html.Div([
                html.P("전체 공고", className="s-label"),
                html.P(f"{len(df):,}건", className="s-value"),
                html.P(f"+{new7} 최근 7일", className="s-delta"),
            ], className="sidebar-metric"),
            html.Div([
                html.P("마지막 업데이트", className="s-label"),
                html.P(last, className="s-value"),
            ], className="sidebar-metric"),
        ]

    @app.callback(
        Output("kpi-row", "children"),
        Input("filter-categories", "value"),
        Input("filter-sources", "value"),
        Input("filter-industry", "value"),
        Input("filter-emp-type", "value"),
        Input("data-version", "data"),
    )
    def update_kpis(categories, sources, industries, emp_types, _version):
        df = apply_filter(get_jobs_df(), categories, sources, industries, emp_types)

        # 소스별 공고 수
        src_counts = {s: int((df["source_site"] == s).sum()) for s in ["wanted", "saramin", "jobkorea"]}
        src_labels = {"wanted": "원티드", "saramin": "사람인", "jobkorea": "잡코리아"}

        return [
            kpi_card("활성 공고", f"{len(df):,}건"),
            *[kpi_card(src_labels[s], f"{src_counts[s]:,}건") for s in src_labels],
        ]
