"""트렌드 탭 콜백 (주별 추이 · 스킬 수요 · 경력 분포)."""
import pandas as pd
import plotly.express as px
from dash import Input, Output, dcc, html

from analysis import (
    weekly_job_counts,
    skill_trend_weekly,
    experience_distribution,
)
from dashboard.context import JOBS_DF, SKILLS_DF, PALETTE
from dashboard.utils import apply_filter, chart_base, empty_fig, section_wrap


def register(app) -> None:

    @app.callback(
        Output("trend-content", "children"),
        Input("filter-categories", "value"),
        Input("filter-sources", "value"),
        Input("filter-industry", "value"),
        Input("filter-emp-type", "value"),
    )
    def update_trend(categories, sources, industries, emp_types):
        df = apply_filter(JOBS_DF, categories, sources, industries, emp_types)
        sf = apply_filter(SKILLS_DF, categories, sources)

        # ── 주별 추이 ─────────────────────────────────────────────
        weekly = weekly_job_counts(df)
        if not weekly.empty:
            n_weeks = weekly["week"].nunique()
            if n_weeks < 2:
                _fig1 = px.bar(
                    weekly, x="job_category", y="count", color="job_category",
                    labels={"job_category": "직군", "count": "공고 수"},
                    color_discrete_sequence=PALETTE, text="count",
                )
                _fig1.update_traces(textposition="outside")
            else:
                _fig1 = px.line(
                    weekly, x="week", y="count", color="job_category", markers=True,
                    labels={"week": "주", "count": "공고 수", "job_category": "직군"},
                    color_discrete_sequence=PALETTE,
                )
            fig1 = chart_base(_fig1)
            fig1.update_yaxes(title_text="")
        else:
            fig1 = empty_fig()

        # ── 스킬 트렌드 (상위 5개) ────────────────────────────────
        top5 = (
            sf.groupby("skill_name").size().sort_values(ascending=False)
            .head(5).index.tolist()
        ) if not sf.empty else []
        skill_w = skill_trend_weekly(sf, top5) if top5 else pd.DataFrame()
        if not skill_w.empty:
            n_weeks_s = skill_w["week"].nunique()
            if n_weeks_s < 2:
                _fig2 = px.bar(
                    skill_w, x="skill_name", y="count", color="skill_name",
                    labels={"skill_name": "스킬", "count": "언급 공고 수"},
                    color_discrete_sequence=PALETTE, text="count",
                )
                _fig2.update_traces(textposition="outside")
            else:
                _fig2 = px.line(
                    skill_w, x="week", y="count", color="skill_name", markers=True,
                    labels={"week": "주", "count": "언급 공고 수", "skill_name": "스킬"},
                    color_discrete_sequence=PALETTE,
                )
            fig2 = chart_base(_fig2, height=320)
            fig2.update_yaxes(title_text="")
        else:
            fig2 = empty_fig("스킬 데이터 없음")

        # ── 경력 분포 ─────────────────────────────────────────────
        exp_df = experience_distribution(df)
        fig3 = chart_base(
            px.bar(
                exp_df, x="exp_group", y="count", color="job_category", barmode="group",
                labels={"exp_group": "경력", "count": "공고 수", "job_category": "직군"},
                color_discrete_sequence=PALETTE,
            ) if not exp_df.empty else empty_fig(),
            height=300,
        )

        top5_label = ", ".join(top5) if top5 else "없음"
        return [
            section_wrap("주별 채용공고 수 추이", dcc.Graph(figure=fig1)),
            section_wrap(f"스킬 수요 트렌드 (상위 5개: {top5_label})", dcc.Graph(figure=fig2)),
            section_wrap("경력 요건 분포", dcc.Graph(figure=fig3)),
        ]
