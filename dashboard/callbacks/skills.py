"""기술 스택 탭 콜백 (TOP N 바 차트 · 급상승 · 순위 테이블)."""
import plotly.express as px
from dash import Input, Output, dash_table

from analysis import top_skills_by_category, skill_growth_rate
from dashboard.context import JOBS_DF, SKILLS_DF, ALL_CATEGORIES, BLUE, BLUE_LIGHT, WHITE
from dashboard.utils import apply_filter, empty_fig


def register(app) -> None:

    @app.callback(
        Output("skill-cat", "options"),
        Output("skill-cat", "value"),
        Input("filter-categories", "value"),
    )
    def update_skill_cat_options(categories):
        opts = [{"label": c, "value": c} for c in (categories or ALL_CATEGORIES)]
        default = opts[0]["value"] if opts else None
        return opts, default

    @app.callback(
        Output("skill-bar-graph", "figure"),
        Input("filter-categories", "value"),
        Input("filter-sources", "value"),
        Input("skill-cat", "value"),
        Input("skill-top-n", "value"),
    )
    def update_skill_bar(categories, sources, sel_cat, top_n):
        sf = apply_filter(SKILLS_DF, categories, sources)
        df = apply_filter(JOBS_DF, categories, sources)
        top_n = top_n or 20
        # pct 분모를 jobs_df 기준으로 계산 (직군 내 공고 수 비율)
        top_skills = top_skills_by_category(sf, jobs_df=df, top_n=top_n)
        if sel_cat and not top_skills.empty:
            top_skills = top_skills[top_skills["job_category"] == sel_cat]
        if top_skills.empty:
            return empty_fig()
        fig = px.bar(
            top_skills.sort_values("count"),
            x="count", y="skill_name", orientation="h", text="pct",
            labels={"count": "공고 수", "skill_name": "", "pct": "비율(%)"},
            color="count", color_continuous_scale=[BLUE_LIGHT, BLUE],
            height=max(320, top_n * 22),
        )
        fig.update_traces(texttemplate="%{text}%", textposition="outside")
        fig.update_layout(
            coloraxis_showscale=False, plot_bgcolor=WHITE,
            paper_bgcolor=WHITE, margin=dict(l=0, r=40, t=10, b=0),
        )
        fig.update_xaxes(showgrid=False)
        return fig

    @app.callback(
        Output("skill-growth-graph", "figure"),
        Output("skill-rank-table", "children"),
        Input("filter-categories", "value"),
        Input("filter-sources", "value"),
    )
    def update_skill_right(categories, sources):
        sf = apply_filter(SKILLS_DF, categories, sources)

        growth = skill_growth_rate(sf)
        if not growth.empty:
            has_prev = (growth["prev"] > 0).any()
            x_col    = "growth_pct" if has_prev else "recent"
            x_label  = "증감률 (%)" if has_prev else "최근 2주 언급 공고 수"
            fig = px.bar(
                growth.head(10).sort_values(x_col),
                x=x_col, y="skill_name", orientation="h",
                labels={x_col: x_label, "skill_name": ""},
                color=x_col, color_continuous_scale=[BLUE_LIGHT, BLUE], height=300,
            )
            fig.update_layout(
                coloraxis_showscale=False, plot_bgcolor=WHITE,
                paper_bgcolor=WHITE, margin=dict(l=0, r=0, t=10, b=0),
            )
            fig.update_xaxes(showgrid=False)
        else:
            fig = empty_fig("4주 이상 데이터 필요")

        rank = (
            sf.groupby("skill_name").size()
            .reset_index(name="공고 수")
            .sort_values("공고 수", ascending=False)
            .head(25)
            .reset_index(drop=True)
            .rename(columns={"skill_name": "스킬"})
        )
        rank.index += 1
        table = dash_table.DataTable(
            data=rank.reset_index().rename(columns={"index": "#"}).to_dict("records"),
            columns=[{"name": c, "id": c} for c in ["#", "스킬", "공고 수"]],
            style_table={"height": "280px", "overflowY": "auto"},
            style_header={
                "background": BLUE_LIGHT, "color": BLUE,
                "fontWeight": 600, "fontSize": "0.82rem",
            },
            style_cell={
                "fontSize": "0.82rem", "padding": "6px 10px",
                "fontFamily": "inherit", "border": "1px solid #e1e5ec",
            },
            style_data_conditional=[{"if": {"row_index": "odd"}, "background": "#f9fafc"}],
        )
        return fig, table
