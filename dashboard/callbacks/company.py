"""기업 분석 탭 콜백 (상위 기업 · 지역 바 차트 · 직군×지역 히트맵)."""
import pandas as pd
import plotly.express as px
from dash import Input, Output

from analysis import company_rankings, location_distribution
from dashboard.context import JOBS_DF, BLUE, BLUE_LIGHT, WHITE
from dashboard.utils import apply_filter, chart_base, empty_fig


def register(app) -> None:

    @app.callback(
        Output("company-bar-graph", "figure"),
        Output("location-bar-graph", "figure"),
        Output("location-heatmap", "figure"),
        Input("filter-categories", "value"),
        Input("filter-sources", "value"),
        Input("filter-industry", "value"),
        Input("filter-emp-type", "value"),
        Input("company-top-n", "value"),
    )
    def update_company(categories, sources, industries, emp_types, top_n):
        df = apply_filter(JOBS_DF, categories, sources, industries, emp_types)
        top_n  = top_n or 20
        co_df  = company_rankings(df, top_n=top_n)
        loc_df = location_distribution(df)

        # 상위 기업 바 차트
        if not co_df.empty:
            fig_co = chart_base(
                px.bar(
                    co_df.sort_values("count"),
                    x="count", y="company_name", orientation="h",
                    labels={"count": "공고 수", "company_name": ""},
                    color="count", color_continuous_scale=[BLUE_LIGHT, BLUE],
                    hover_data=["categories"],
                    height=max(350, top_n * 22),
                ),
                height=max(350, top_n * 22),
            )
            fig_co.update_layout(coloraxis_showscale=False, margin=dict(l=0, r=20, t=10, b=0))
            fig_co.update_xaxes(showgrid=False)
        else:
            fig_co = empty_fig()

        # 지역별 공고 수 바 차트
        if not loc_df.empty:
            city_total = (
                loc_df.groupby("city")["count"]
                .sum().sort_values(ascending=False)
                .head(12).reset_index()
            )
            fig_loc = chart_base(
                px.bar(
                    city_total, x="count", y="city", orientation="h",
                    labels={"count": "공고 수", "city": ""},
                    color="count", color_continuous_scale=[BLUE_LIGHT, BLUE], height=300,
                ),
                height=300,
            )
            fig_loc.update_layout(coloraxis_showscale=False, margin=dict(l=0, r=20, t=10, b=0))
        else:
            fig_loc = empty_fig()

        # 직군 × 지역 히트맵
        if not loc_df.empty:
            pivot = loc_df.pivot_table(
                index="city", columns="job_category", values="count", fill_value=0
            ).head(10)
            pivot.index.name  = "지역"
            pivot.columns.name = "직군"
            fig_heat = px.imshow(
                pivot,
                color_continuous_scale=["white", BLUE],
                labels={"color": "공고 수", "x": "직군", "y": "지역"},
                aspect="auto", height=260,
            )
            fig_heat.update_layout(
                margin=dict(l=0, r=0, t=10, b=0), paper_bgcolor=WHITE,
                coloraxis_colorbar=dict(thickness=10),
            )
            fig_heat.update_xaxes(title_text="")
            fig_heat.update_yaxes(title_text="")
        else:
            fig_heat = empty_fig()

        return fig_co, fig_loc, fig_heat
