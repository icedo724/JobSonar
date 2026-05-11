"""사이드바 메트릭 + KPI 카드 콜백."""
import pandas as pd
from dash import Input, Output, html

from analysis import new_jobs_count, salary_by_category
from dashboard.context import JOBS_DF, SKILLS_DF, BLUE
from dashboard.utils import apply_filter, kpi_card


def register(app) -> None:

    @app.callback(
        Output("sidebar-metrics", "children"),
        Input("filter-categories", "value"),
        Input("filter-sources", "value"),
        Input("filter-industry", "value"),
        Input("filter-emp-type", "value"),
    )
    def update_sidebar(categories, sources, industries, emp_types):
        df = apply_filter(JOBS_DF, categories, sources, industries, emp_types)
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
    )
    def update_kpis(categories, sources, industries, emp_types):
        df = apply_filter(JOBS_DF, categories, sources, industries, emp_types)
        sf = apply_filter(SKILLS_DF, categories, sources)

        top_skill = "—"
        if not sf.empty:
            top_skill = sf.groupby("skill_name").size().idxmax()

        sal_df = salary_by_category(df)
        avg_sal = (
            f"{int(sal_df['salary_mid'].median()):,}만원"
            if not sal_df.empty else "정보 없음"
        )

        today = pd.Timestamp.now().normalize()
        deadline_soon = 0
        if "deadline_date" in df.columns:
            d = df["deadline_date"].dropna()
            deadline_soon = int(((d >= today) & (d <= today + pd.Timedelta(days=7))).sum())

        return [
            kpi_card("활성 공고", f"{len(df):,}건"),
            kpi_card("마감 임박 7일", f"{deadline_soon:,}건"),
            kpi_card("가장 요구된 스킬", top_skill),
            kpi_card("연봉 중간값", avg_sal),
        ]
