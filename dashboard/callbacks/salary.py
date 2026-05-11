"""연봉 분석 탭 콜백 (박스플롯 · 히스토그램 · 통계 테이블)."""
import pandas as pd
import plotly.express as px
from dash import Input, Output, dash_table, dcc, html

from analysis import salary_by_category
from dashboard.context import JOBS_DF, BLUE, BLUE_LIGHT, WHITE, GRAY, PALETTE
from dashboard.utils import apply_filter, chart_base, section_wrap


def register(app) -> None:

    @app.callback(
        Output("salary-content", "children"),
        Input("filter-categories", "value"),
        Input("filter-sources", "value"),
    )
    def update_salary(categories, sources):
        df = apply_filter(JOBS_DF, categories, sources)
        sal_df = salary_by_category(df)

        # 연봉 공개 비율 요약
        total_cnt = len(df)
        disc_cnt  = len(df.dropna(subset=["salary_min"]))
        disc_pct  = round(disc_cnt / total_cnt * 100, 1) if total_cnt else 0
        disc_summary = html.Div([
            html.Span(
                f"연봉 공개 {disc_cnt:,}건 ({disc_pct}%)",
                style={"fontWeight": 600, "color": BLUE},
            ),
            html.Span(
                f"  /  비공개 {total_cnt - disc_cnt:,}건",
                style={"color": GRAY, "marginLeft": "8px"},
            ),
        ], style={"fontSize": "0.85rem", "marginBottom": "16px"})

        if sal_df.empty:
            return html.Div([
                disc_summary,
                html.P(
                    "연봉 공개 공고가 아직 없습니다. 누적 데이터가 쌓이면 분석이 가능합니다.",
                    className="no-data",
                ),
            ])

        fig_box = chart_base(
            px.box(
                sal_df, x="job_category", y="salary_mid", color="job_category",
                points="all",
                labels={"job_category": "직군", "salary_mid": "연봉 중간값 (만원)"},
                color_discrete_sequence=PALETTE, hover_data=["company_name"],
            ),
            height=400,
        )
        fig_box.update_layout(showlegend=False)

        fig_hist = chart_base(
            px.histogram(
                sal_df, x="salary_mid", color="job_category", nbins=20, opacity=0.75,
                labels={"salary_mid": "연봉 (만원)", "job_category": "직군"},
                color_discrete_sequence=PALETTE, barmode="overlay",
            ),
            height=260,
        )

        stat = (
            sal_df.groupby("job_category")["salary_mid"]
            .agg(["median", "mean", "min", "max", "count"])
            .round(0).astype(int)
            .rename(columns={
                "median": "중간값", "mean": "평균",
                "min": "최저", "max": "최고", "count": "샘플 수",
            })
            .reset_index()
            .rename(columns={"job_category": "직군"})
        )
        table = dash_table.DataTable(
            data=stat.to_dict("records"),
            columns=[{"name": c, "id": c} for c in stat.columns],
            style_header={
                "background": BLUE_LIGHT, "color": BLUE,
                "fontWeight": 600, "fontSize": "0.82rem",
            },
            style_cell={
                "fontSize": "0.82rem", "padding": "7px 12px",
                "fontFamily": "inherit", "border": "1px solid #e1e5ec",
            },
            style_data_conditional=[{"if": {"row_index": "odd"}, "background": "#f9fafc"}],
        )

        return html.Div([
            disc_summary,
            html.Div([
                html.Div([
                    section_wrap("직군별 연봉 분포", dcc.Graph(figure=fig_box)),
                ], style={"flex": "3"}),
                html.Div([
                    section_wrap("직군별 연봉 통계", table),
                    section_wrap("연봉 분포 히스토그램", dcc.Graph(figure=fig_hist)),
                    html.P(
                        "* salary_mid = (최소+최대)/2. 연봉 미기재 제외.",
                        style={"fontSize": "0.75rem", "color": GRAY, "marginTop": "6px"},
                    ),
                ], style={"flex": "2"}),
            ], style={"display": "flex", "gap": "16px"}),
        ])
