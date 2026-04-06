"""JobSonar — Dash 대시보드. 실행: python dashboard/app.py"""
import os
import sys
import math
import shutil
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd
from dash import Dash, dcc, html, Input, Output, State, dash_table
import plotly.express as px
import plotly.graph_objects as go

from db.connection import get_conn, init_db, DB_PATH
from analysis import (
    load_jobs_df, load_skills_df, load_jobs_for_board,
    weekly_job_counts, top_skills_by_category, skill_trend_weekly,
    salary_by_category, company_rankings, location_distribution,
    experience_distribution, skill_growth_rate, new_jobs_count,
    build_cooccurrence_graph, graph_to_plotly_traces,
)

# ── 색상 상수 ────────────────────────────────────────────────────
BLUE       = "#1352f1"
BLUE_LIGHT = "#e8f0fe"
WHITE      = "#ffffff"
GRAY       = "#6b7684"
PALETTE    = ["#1352f1", "#4f7ef7", "#0abf7a", "#f5a623", "#e83e3e"]

# ── HF Dataset DB 다운로드 ───────────────────────────────────────
HF_DATASET_REPO = os.getenv("HF_DATASET_REPO", "")

def ensure_db():
    if DB_PATH.exists():
        return
    if not HF_DATASET_REPO:
        return
    try:
        from huggingface_hub import hf_hub_download
        cached = hf_hub_download(
            repo_id=HF_DATASET_REPO, filename="jobsonar.db", repo_type="dataset"
        )
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(cached, DB_PATH)
    except Exception as e:
        print(f"[warn] DB 다운로드 실패: {e}")


ensure_db()
init_db()

# ── 데이터 로드 (시작 시 1회) ────────────────────────────────────
with get_conn() as _conn:
    JOBS_DF   = load_jobs_df(_conn)
    SKILLS_DF = load_skills_df(_conn)
    BOARD_DF  = load_jobs_for_board(_conn)

ALL_CATEGORIES = sorted(JOBS_DF["job_category"].dropna().unique().tolist())
ALL_SOURCES    = ["wanted", "saramin", "jobkorea"]
HAS_DATA       = len(JOBS_DF) > 0

# ── 업종 목록 (DB에서 직접 수집된 값 사용) ───────────────────────
ALL_INDUSTRIES = sorted(
    JOBS_DF["industry"].dropna().unique().tolist()
) if "industry" in JOBS_DF.columns else []

# ── 헬퍼 ─────────────────────────────────────────────────────────

def apply_filter(df: pd.DataFrame, categories: list, sources: list,
                 industries: list | None = None) -> pd.DataFrame:
    if df.empty:
        return df
    mask = pd.Series(True, index=df.index)
    if categories and "job_category" in df.columns:
        mask &= df["job_category"].isin(categories)
    if sources and "source_site" in df.columns:
        mask &= df["source_site"].isin(sources)
    if industries and "industry" in df.columns:
        mask &= df["industry"].isin(industries)
    return df[mask]


def chart_base(fig, height=380) -> go.Figure:
    fig.update_layout(
        height=height, plot_bgcolor=WHITE, paper_bgcolor=WHITE,
        margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        font=dict(family="Pretendard, Apple SD Gothic Neo, sans-serif"),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="#f0f0f0")
    return fig


def section_wrap(title: str, *children):
    return html.Div([html.P(title, className="section-title"), *children], className="chart-section")


def kpi_card(label: str, value: str, delta: str | None = None):
    return html.Div([
        html.P(label, className="kpi-label"),
        html.P(value, className="kpi-value"),
        html.P(delta, className="kpi-delta") if delta else None,
    ], className="kpi-card")


def source_badge(source: str):
    cfg = {
        "wanted":   ("#e8f4fd", "#1352f1", "원티드"),
        "saramin":  ("#f0f8f0", "#1a7340", "사람인"),
        "jobkorea": ("#fff3e0", "#e65100", "잡코리아"),
    }
    bg, color, label = cfg.get(source, ("#f0f0f0", "#666", source))
    return html.Span(label, style={
        "background": bg, "color": color,
        "fontSize": "0.72rem", "fontWeight": 600,
        "padding": "2px 8px", "borderRadius": "20px",
    })


_REGIONS = ["서울", "경기", "인천", "부산", "대구", "대전", "광주", "울산", "세종",
            "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주"]

def normalize_location(loc) -> str:
    if not loc or (isinstance(loc, float) and pd.isna(loc)):
        return ""
    s = str(loc)
    for r in _REGIONS:
        if s.startswith(r) or r in s:
            return r
    return "해외"


def exp_label(mn, mx) -> str:
    if mn is None or (isinstance(mn, float) and pd.isna(mn)):
        return "경력무관"
    mn, mx = int(mn), (int(mx) if mx is not None and not (isinstance(mx, float) and pd.isna(mx)) else None)
    if mn == 0 and mx == 0:
        return "신입"
    return f"{mn}~{mx}년" if mx else f"{mn}년 이상"


def salary_label(mn, mx) -> str:
    if mn is None or (isinstance(mn, float) and pd.isna(mn)):
        return "연봉 협의"
    mx_valid = mx is not None and not (isinstance(mx, float) and pd.isna(mx))
    return f"{int(mn):,}~{int(mx):,}만원" if mx_valid else f"{int(mn):,}만원~"


def empty_fig(msg="데이터 없음") -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(text=msg, x=0.5, y=0.5, xref="paper", yref="paper",
                       showarrow=False, font=dict(size=14, color=GRAY))
    fig.update_layout(height=300, plot_bgcolor=WHITE, paper_bgcolor=WHITE,
                      xaxis=dict(visible=False), yaxis=dict(visible=False))
    return fig


# ── 탭 스타일 ────────────────────────────────────────────────────
_TAB = dict(padding="10px 20px", color=GRAY, fontWeight=500,
            fontSize="0.9rem", background=WHITE, borderBottom=f"2px solid transparent")
_TAB_SEL = {**_TAB, "color": BLUE, "borderBottom": f"2px solid {BLUE}",
            "fontWeight": 700, "background": BLUE_LIGHT}

# ── Dash 앱 초기화 ───────────────────────────────────────────────
app = Dash(__name__, assets_folder="assets", suppress_callback_exceptions=True)
server = app.server  # gunicorn 진입점

# ── 레이아웃 ─────────────────────────────────────────────────────
app.layout = html.Div([

    # 사이드바
    html.Div([
        html.Div([
            html.Span("📡", style={"fontSize": "1.4rem"}),
            html.Span("JobSonar", style={"fontSize": "1.2rem", "fontWeight": 800, "marginLeft": "8px"}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "4px"}),
        html.P("한국 IT 채용 시장 트렌드", className="sidebar-sub"),

        html.Hr(className="sidebar-hr"),
        html.Label("직군", className="filter-label"),
        dcc.Checklist(
            id="filter-categories",
            options=[{"label": c, "value": c} for c in ALL_CATEGORIES],
            value=ALL_CATEGORIES,
            className="filter-checklist",
            labelStyle={"display": "flex", "alignItems": "center",
                        "gap": "6px", "marginBottom": "5px", "fontSize": "0.85rem"},
        ),

        html.Hr(className="sidebar-hr"),
        html.Label("사이트", className="filter-label"),
        dcc.Checklist(
            id="filter-sources",
            options=[
                {"label": "원티드",   "value": "wanted"},
                {"label": "사람인",   "value": "saramin"},
                {"label": "잡코리아", "value": "jobkorea"},
            ],
            value=ALL_SOURCES,
            className="filter-checklist",
            labelStyle={"display": "flex", "alignItems": "center",
                        "gap": "6px", "marginBottom": "5px", "fontSize": "0.85rem"},
        ),

        html.Hr(className="sidebar-hr"),
        html.Label("업종", className="filter-label"),
        dcc.Dropdown(
            id="filter-industry",
            options=[{"label": i, "value": i} for i in ALL_INDUSTRIES],
            placeholder="전체 업종",
            multi=True,
            clearable=True,
            className="filter-dropdown",
            style={"fontSize": "0.82rem"},
        ),

        html.Hr(className="sidebar-hr"),
        html.Div(id="sidebar-metrics"),
        html.Hr(className="sidebar-hr"),
        html.P("원티드 · 사람인 · 잡코리아", className="sidebar-caption"),
        html.P("매일 오전 10시 갱신", className="sidebar-caption"),
    ], className="sidebar"),

    # 메인 영역
    html.Div([
        # 헤더 배너
        html.Div([
            html.H1("📡 JobSonar"),
            html.P("한국 IT 채용 시장 트렌드 분석 · 원티드 · 사람인 · 잡코리아"),
        ], className="header-banner"),

        # KPI
        html.Div(id="kpi-row", className="kpi-row"),

        # 탭
        dcc.Tabs(id="main-tabs", value="board", className="tabs-container", children=[

            # ── 공고 목록 ─────────────────────────────────────────
            dcc.Tab(label="📋 공고 목록", value="board", style=_TAB, selected_style=_TAB_SEL, children=[
                html.Div([
                    html.Div([
                        dcc.Input(id="board-search", placeholder="🔍  공고명 / 회사명 / 기술스택",
                                  type="text", debounce=True, className="search-input"),
                        dcc.Dropdown(id="board-location", placeholder="지역", clearable=True,
                                     className="filter-dropdown"),
                        dcc.Dropdown(id="board-exp", placeholder="경력", clearable=True,
                                     options=["신입", "1-3년", "3-5년", "5년+"],
                                     className="filter-dropdown"),
                    ], className="board-filters"),
                    html.P(id="board-count", className="board-count"),
                    html.Div(id="board-cards"),
                    html.Div([
                        html.Button("◀", id="board-prev", n_clicks=0, className="page-btn"),
                        html.Span(id="board-page-info"),
                        html.Button("▶", id="board-next", n_clicks=0, className="page-btn"),
                    ], className="pagination"),
                    dcc.Store(id="board-page", data=1),
                ], className="tab-inner"),
            ]),

            # ── 트렌드 ────────────────────────────────────────────
            dcc.Tab(label="📈 트렌드", value="trend", style=_TAB, selected_style=_TAB_SEL, children=[
                html.Div(id="trend-content", className="tab-inner"),
            ]),

            # ── 기술 스택 ─────────────────────────────────────────
            dcc.Tab(label="🔧 기술 스택", value="skills", style=_TAB, selected_style=_TAB_SEL, children=[
                html.Div([
                    html.Div([
                        # 좌: TOP N 바 차트
                        html.Div([
                            section_wrap("직군별 요구 기술 TOP N",
                                html.Div([
                                    dcc.Dropdown(id="skill-cat", placeholder="직군 선택",
                                                 className="filter-dropdown",
                                                 style={"marginBottom": "12px"}),
                                    dcc.Slider(id="skill-top-n", min=5, max=30, step=5, value=20,
                                               marks={5:"5", 10:"10", 15:"15", 20:"20", 25:"25", 30:"30"}),
                                    dcc.Graph(id="skill-bar-graph"),
                                ]),
                            ),
                        ], style={"flex": "3"}),
                        # 우: 급상승 + 순위 테이블
                        html.Div([
                            section_wrap("스킬 급상승 (최근 2주)", dcc.Graph(id="skill-growth-graph")),
                            section_wrap("전체 스킬 순위", html.Div(id="skill-rank-table")),
                        ], style={"flex": "2"}),
                    ], style={"display": "flex", "gap": "16px"}),
                ], className="tab-inner"),
            ]),

            # ── 스킬 네트워크 ─────────────────────────────────────
            dcc.Tab(label="🕸️ 스킬 네트워크", value="network", style=_TAB, selected_style=_TAB_SEL, children=[
                html.Div([
                    html.Div([
                        dcc.Dropdown(id="net-category", placeholder="직군 (전체)", clearable=True,
                                     options=[{"label": c, "value": c} for c in ALL_CATEGORIES],
                                     className="filter-dropdown",
                                     style={"width": "240px"}),
                        html.Div([
                            html.Label("최소 공동 출현 횟수",
                                       style={"fontSize": "0.83rem", "color": GRAY, "marginBottom": "4px"}),
                            dcc.Slider(id="net-min-cooccur", min=2, max=20, step=1, value=3,
                                       marks={2:"2", 5:"5", 10:"10", 15:"15", 20:"20"}),
                        ], style={"flex": "1"}),
                    ], style={"display": "flex", "gap": "20px", "alignItems": "flex-end", "marginBottom": "14px"}),
                    html.Div(id="network-metrics",
                             style={"display": "flex", "gap": "16px", "marginBottom": "12px"}),
                    dcc.Graph(id="network-graph"),
                ], className="tab-inner"),
            ]),

            # ── 연봉 분석 ─────────────────────────────────────────
            dcc.Tab(label="💰 연봉 분석", value="salary", style=_TAB, selected_style=_TAB_SEL, children=[
                html.Div(id="salary-content", className="tab-inner"),
            ]),

            # ── 기업 분석 ─────────────────────────────────────────
            dcc.Tab(label="🏢 기업 분석", value="company", style=_TAB, selected_style=_TAB_SEL, children=[
                html.Div([
                    html.Div([
                        html.Div([
                            section_wrap("채용 공고 상위 기업",
                                html.Div([
                                    dcc.Slider(id="company-top-n", min=10, max=40, step=5, value=20,
                                               marks={10:"10", 20:"20", 30:"30", 40:"40"}),
                                    html.Div(style={"marginBottom": "12px"}),
                                    dcc.Graph(id="company-bar-graph"),
                                ]),
                            ),
                        ], style={"flex": "3"}),
                        html.Div([
                            section_wrap("지역별 공고 수", dcc.Graph(id="location-bar-graph")),
                            section_wrap("직군 × 지역 히트맵", dcc.Graph(id="location-heatmap")),
                        ], style={"flex": "2"}),
                    ], style={"display": "flex", "gap": "16px"}),
                ], className="tab-inner"),
            ]),
        ]),
    ], className="main"),
], className="app-wrapper")


# ════════════════════════════════════════════════════════════════
# 콜백
# ════════════════════════════════════════════════════════════════

@app.callback(Output("sidebar-metrics", "children"),
              Input("filter-categories", "value"),
              Input("filter-sources", "value"),
              Input("filter-industry", "value"))
def update_sidebar(categories, sources, industries):
    df = apply_filter(JOBS_DF, categories, sources, industries)
    new7 = new_jobs_count(df, days=7)
    last = df["collected_at"].max().strftime("%Y-%m-%d") if not df.empty else "—"
    return [
        html.Div([html.P("전체 공고", className="s-label"),
                  html.P(f"{len(df):,}건", className="s-value"),
                  html.P(f"+{new7} 최근 7일", className="s-delta")],
                 className="sidebar-metric"),
        html.Div([html.P("마지막 업데이트", className="s-label"),
                  html.P(last, className="s-value")],
                 className="sidebar-metric"),
    ]


@app.callback(Output("kpi-row", "children"),
              Input("filter-categories", "value"),
              Input("filter-sources", "value"),
              Input("filter-industry", "value"))
def update_kpis(categories, sources, industries):
    df = apply_filter(JOBS_DF, categories, sources, industries)
    sf = apply_filter(SKILLS_DF, categories, sources)
    new7 = new_jobs_count(df, days=7)

    top_skill = "—"
    if not sf.empty:
        top_skill = sf.groupby("skill_name").size().idxmax()

    sal_df = salary_by_category(df)
    avg_sal = f"{int(sal_df['salary_mid'].median()):,}만원" if not sal_df.empty else "정보 없음"

    return [
        kpi_card("활성 공고", f"{len(df):,}건"),
        kpi_card("최근 7일 신규", f"{new7:,}건"),
        kpi_card("가장 요구된 스킬", top_skill),
        kpi_card("연봉 중간값", avg_sal),
    ]


# ── 공고 목록 ────────────────────────────────────────────────────

@app.callback(Output("board-location", "options"),
              Input("filter-categories", "value"),
              Input("filter-sources", "value"),
              Input("filter-industry", "value"))
def update_location_options(categories, sources, industries):
    df = apply_filter(BOARD_DF, categories, sources, industries)
    locs = sorted({normalize_location(l) for l in df["location"].dropna()} - {""})
    return [{"label": l, "value": l} for l in locs]


@app.callback(Output("board-page", "data"),
              Input("board-prev", "n_clicks"),
              Input("board-next", "n_clicks"),
              Input("board-search", "value"),
              Input("board-location", "value"),
              Input("board-exp", "value"),
              Input("filter-categories", "value"),
              Input("filter-sources", "value"),
              Input("filter-industry", "value"),
              State("board-page", "data"))
def update_page(prev, nxt, search, location, exp, categories, sources, industries, current):
    # 필터 변경 시 1페이지로 리셋
    from dash import ctx
    trigger = ctx.triggered_id
    if trigger in ("board-search", "board-location", "board-exp",
                   "filter-categories", "filter-sources", "filter-industry"):
        return 1
    if trigger == "board-prev":
        return max(1, current - 1)
    if trigger == "board-next":
        return current + 1
    return 1


@app.callback(Output("board-cards", "children"),
              Output("board-count", "children"),
              Output("board-page-info", "children"),
              Input("filter-categories", "value"),
              Input("filter-sources", "value"),
              Input("filter-industry", "value"),
              Input("board-search", "value"),
              Input("board-location", "value"),
              Input("board-exp", "value"),
              Input("board-page", "data"))
def update_board(categories, sources, industries, keyword, location, exp, page):
    PAGE_SIZE = 20
    if not categories or not sources:
        return [html.P("필터를 선택해 주세요.", className="no-data")], "총 0건", "1 / 1"
    df = apply_filter(BOARD_DF, categories, sources, industries)

    if keyword:
        kw = keyword.lower()
        df = df[
            df["title"].str.lower().str.contains(kw, na=False) |
            df["company_name"].str.lower().str.contains(kw, na=False) |
            df["skills"].fillna("").str.lower().str.contains(kw, na=False)
        ]
    if location:
        df = df[df["location"].apply(normalize_location) == location]
    if exp:
        exp_map = {"신입": (0, 0), "1-3년": (1, 3), "3-5년": (3, 5), "5년+": (5, 99)}
        lo, hi = exp_map[exp]
        df = df[df["experience_min"].fillna(-1).between(lo, hi)]

    total = len(df)
    total_pages = max(1, math.ceil(total / PAGE_SIZE))
    page = min(page, total_pages)
    page_data = df.iloc[(page - 1) * PAGE_SIZE: page * PAGE_SIZE]

    cards = []
    for _, row in page_data.iterrows():
        skills_el = html.Div(
            [html.Span(s.strip().title(), className="skill-badge")
             for s in str(row.get("skills", "")).split("·")
             if s.strip() and pd.notna(row.get("skills"))],
            className="job-skills",
        )
        deadline_el = None
        if pd.notna(row.get("deadline_date")):
            deadline_el = html.Span(
                f"마감 {row['deadline_date'].strftime('%m/%d')}",
                style={"color": "#e83e3e", "fontSize": "0.78rem", "fontWeight": 600},
            )
        def _s(v):
            return str(v) if pd.notna(v) and v != "" else ""
        meta = " · ".join(filter(None, [
            normalize_location(row.get("location")) or "",
            exp_label(row.get("experience_min"), row.get("experience_max")),
            salary_label(row.get("salary_min"), row.get("salary_max")),
        ]))

        cards.append(html.Div([
            html.Div([
                html.Div([
                    html.A(row["title"], href=row["url"], target="_blank", className="job-title"),
                    html.P(row["company_name"], className="job-company"),
                ]),
                html.Div([source_badge(row["source_site"]),
                          deadline_el or html.Span()],
                         style={"display": "flex", "alignItems": "center", "gap": "6px"}),
            ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "flex-start"}),
            html.P(meta, className="job-meta"),
            skills_el,
        ], className="job-card"))

    count_text = f"총 {total:,}건"
    page_info = f"{page} / {total_pages}"
    return cards or [html.P("조건에 맞는 공고가 없습니다.", className="no-data")], count_text, page_info


# ── 트렌드 ──────────────────────────────────────────────────────

@app.callback(Output("trend-content", "children"),
              Input("filter-categories", "value"),
              Input("filter-sources", "value"),
              Input("filter-industry", "value"))
def update_trend(categories, sources, industries):
    df = apply_filter(JOBS_DF, categories, sources, industries)
    sf = apply_filter(SKILLS_DF, categories, sources)

    # 주별 추이
    weekly = weekly_job_counts(df)
    fig1 = chart_base(
        px.line(weekly, x="week", y="count", color="job_category", markers=True,
                labels={"week": "", "count": "공고 수", "job_category": "직군"},
                color_discrete_sequence=PALETTE)
        if not weekly.empty else empty_fig()
    )

    # 스킬 트렌드 (상위 5개)
    top5 = (sf.groupby("skill_name").size().sort_values(ascending=False)
            .head(5).index.tolist()) if not sf.empty else []
    skill_w = skill_trend_weekly(sf, top5) if top5 else pd.DataFrame()
    fig2 = chart_base(
        px.line(skill_w, x="week", y="count", color="skill_name", markers=True,
                labels={"week": "", "count": "언급 공고 수", "skill_name": "스킬"},
                color_discrete_sequence=PALETTE)
        if not skill_w.empty else empty_fig("스킬 데이터 없음"), height=320
    )

    # 경력 분포
    exp_df = experience_distribution(df)
    fig3 = chart_base(
        px.bar(exp_df, x="exp_group", y="count", color="job_category", barmode="group",
               labels={"exp_group": "경력", "count": "공고 수", "job_category": "직군"},
               color_discrete_sequence=PALETTE)
        if not exp_df.empty else empty_fig(), height=300
    )

    return [
        section_wrap("주별 채용공고 수 추이", dcc.Graph(figure=fig1)),
        section_wrap(f"스킬 수요 트렌드 (상위 5개: {', '.join(top5)})", dcc.Graph(figure=fig2)),
        section_wrap("경력 요건 분포", dcc.Graph(figure=fig3)),
    ]


# ── 기술 스택 ────────────────────────────────────────────────────

@app.callback(Output("skill-cat", "options"),
              Output("skill-cat", "value"),
              Input("filter-categories", "value"))
def update_skill_cat_options(categories):
    opts = [{"label": c, "value": c} for c in (categories or ALL_CATEGORIES)]
    default = opts[0]["value"] if opts else None
    return opts, default


@app.callback(Output("skill-bar-graph", "figure"),
              Input("filter-categories", "value"),
              Input("filter-sources", "value"),
              Input("skill-cat", "value"),
              Input("skill-top-n", "value"))
def update_skill_bar(categories, sources, sel_cat, top_n):
    sf = apply_filter(SKILLS_DF, categories, sources)
    top_skills = top_skills_by_category(sf, top_n or 20)
    if sel_cat and not top_skills.empty:
        top_skills = top_skills[top_skills["job_category"] == sel_cat]
    if top_skills.empty:
        return empty_fig()
    fig = px.bar(
        top_skills.sort_values("count"),
        x="count", y="skill_name", orientation="h", text="pct",
        labels={"count": "공고 수", "skill_name": "", "pct": "비율(%)"},
        color="count", color_continuous_scale=[BLUE_LIGHT, BLUE],
        height=max(320, (top_n or 20) * 22),
    )
    fig.update_traces(texttemplate="%{text}%", textposition="outside")
    fig.update_layout(coloraxis_showscale=False, plot_bgcolor=WHITE,
                      paper_bgcolor=WHITE, margin=dict(l=0, r=40, t=10, b=0))
    fig.update_xaxes(showgrid=False)
    return fig


@app.callback(Output("skill-growth-graph", "figure"),
              Output("skill-rank-table", "children"),
              Input("filter-categories", "value"),
              Input("filter-sources", "value"))
def update_skill_right(categories, sources):
    sf = apply_filter(SKILLS_DF, categories, sources)

    growth = skill_growth_rate(sf)
    if not growth.empty:
        fig = px.bar(growth.head(10).sort_values("growth_pct"),
                     x="growth_pct", y="skill_name", orientation="h",
                     labels={"growth_pct": "증감률(%)", "skill_name": ""},
                     color="growth_pct", color_continuous_scale=[BLUE_LIGHT, BLUE], height=300)
        fig.update_layout(coloraxis_showscale=False, plot_bgcolor=WHITE,
                          paper_bgcolor=WHITE, margin=dict(l=0, r=0, t=10, b=0))
        fig.update_xaxes(showgrid=False)
    else:
        fig = empty_fig("4주 이상 데이터 필요")

    rank = (sf.groupby("skill_name").size().reset_index(name="공고 수")
            .sort_values("공고 수", ascending=False).head(25).reset_index(drop=True))
    rank.index += 1
    table = dash_table.DataTable(
        data=rank.reset_index().rename(columns={"index": "#"}).to_dict("records"),
        columns=[{"name": c, "id": c} for c in ["#", "skill_name", "공고 수"]],
        style_table={"height": "280px", "overflowY": "auto"},
        style_header={"background": BLUE_LIGHT, "color": BLUE,
                      "fontWeight": 600, "fontSize": "0.82rem"},
        style_cell={"fontSize": "0.82rem", "padding": "6px 10px",
                    "fontFamily": "inherit", "border": f"1px solid #e1e5ec"},
        style_data_conditional=[{"if": {"row_index": "odd"}, "background": "#f9fafc"}],
    )

    return fig, table


# ── 스킬 네트워크 ────────────────────────────────────────────────

@app.callback(Output("network-graph", "figure"),
              Output("network-metrics", "children"),
              Input("net-category", "value"),
              Input("net-min-cooccur", "value"))
def update_network(net_cat, min_cooccur):
    with get_conn() as conn:
        G = build_cooccurrence_graph(conn, category=net_cat, min_cooccur=min_cooccur or 3)

    if len(G.nodes) == 0:
        return empty_fig("조건에 맞는 연결 없음 — 최소 출현 횟수를 낮춰보세요"), []

    metrics = [
        html.Div([html.P("스킬 노드", className="kpi-label"),
                  html.P(str(len(G.nodes)), className="kpi-value")], className="kpi-card"),
        html.Div([html.P("연결 엣지", className="kpi-label"),
                  html.P(str(len(G.edges)), className="kpi-value")], className="kpi-card"),
        html.Div([html.P("평균 연결 수", className="kpi-label"),
                  html.P(f"{sum(d for _,d in G.degree())/len(G.nodes):.1f}",
                         className="kpi-value")], className="kpi-card"),
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


# ── 연봉 분석 ────────────────────────────────────────────────────

@app.callback(Output("salary-content", "children"),
              Input("filter-categories", "value"),
              Input("filter-sources", "value"))
def update_salary(categories, sources):
    df = apply_filter(JOBS_DF, categories, sources)
    sal_df = salary_by_category(df)

    if sal_df.empty:
        return html.P("연봉 데이터가 없습니다. 연봉 비공개 공고가 많을 수 있습니다.", className="no-data")

    fig_box = chart_base(
        px.box(sal_df, x="job_category", y="salary_mid", color="job_category", points="all",
               labels={"job_category": "직군", "salary_mid": "연봉 중간값 (만원)"},
               color_discrete_sequence=PALETTE, hover_data=["company_name"]), height=400
    )
    fig_box.update_layout(showlegend=False)

    fig_hist = chart_base(
        px.histogram(sal_df, x="salary_mid", color="job_category", nbins=20, opacity=0.75,
                     labels={"salary_mid": "연봉 (만원)", "job_category": "직군"},
                     color_discrete_sequence=PALETTE, barmode="overlay"), height=260
    )

    stat = (sal_df.groupby("job_category")["salary_mid"]
            .agg(["median", "mean", "min", "max", "count"]).round(0).astype(int)
            .rename(columns={"median": "중간값", "mean": "평균",
                              "min": "최저", "max": "최고", "count": "샘플 수"})
            .reset_index().rename(columns={"job_category": "직군"}))
    table = dash_table.DataTable(
        data=stat.to_dict("records"),
        columns=[{"name": c, "id": c} for c in stat.columns],
        style_header={"background": BLUE_LIGHT, "color": BLUE, "fontWeight": 600, "fontSize": "0.82rem"},
        style_cell={"fontSize": "0.82rem", "padding": "7px 12px",
                    "fontFamily": "inherit", "border": f"1px solid #e1e5ec"},
        style_data_conditional=[{"if": {"row_index": "odd"}, "background": "#f9fafc"}],
    )

    return html.Div([
        html.Div([
            section_wrap("직군별 연봉 분포", dcc.Graph(figure=fig_box)),
        ], style={"flex": "3"}),
        html.Div([
            section_wrap("직군별 연봉 통계", table),
            section_wrap("연봉 분포 히스토그램", dcc.Graph(figure=fig_hist)),
            html.P("* salary_mid = (최소+최대)/2. 연봉 미기재 제외.",
                   style={"fontSize": "0.75rem", "color": GRAY, "marginTop": "6px"}),
        ], style={"flex": "2"}),
    ], style={"display": "flex", "gap": "16px"})


# ── 기업 분석 ────────────────────────────────────────────────────

@app.callback(Output("company-bar-graph", "figure"),
              Output("location-bar-graph", "figure"),
              Output("location-heatmap", "figure"),
              Input("filter-categories", "value"),
              Input("filter-sources", "value"),
              Input("filter-industry", "value"),
              Input("company-top-n", "value"))
def update_company(categories, sources, industries, top_n):
    df = apply_filter(JOBS_DF, categories, sources, industries)
    co_df = company_rankings(df, top_n=top_n or 20)
    loc_df = location_distribution(df)

    fig_co = (chart_base(
        px.bar(co_df.sort_values("count"), x="count", y="company_name", orientation="h",
               labels={"count": "공고 수", "company_name": ""},
               color="count", color_continuous_scale=[BLUE_LIGHT, BLUE],
               hover_data=["categories"], height=max(350, (top_n or 20) * 22)),
        height=max(350, (top_n or 20) * 22))
        if not co_df.empty else empty_fig())
    if not co_df.empty:
        fig_co.update_layout(coloraxis_showscale=False, margin=dict(l=0, r=20, t=10, b=0))
        fig_co.update_xaxes(showgrid=False)

    city_total = (loc_df.groupby("city")["count"].sum().sort_values(ascending=False)
                  .head(12).reset_index()) if not loc_df.empty else pd.DataFrame()
    fig_loc = (chart_base(
        px.bar(city_total, x="count", y="city", orientation="h",
               labels={"count": "공고 수", "city": ""},
               color="count", color_continuous_scale=[BLUE_LIGHT, BLUE], height=300), height=300)
        if not city_total.empty else empty_fig())
    if not city_total.empty:
        fig_loc.update_layout(coloraxis_showscale=False, margin=dict(l=0, r=20, t=10, b=0))

    if not loc_df.empty:
        pivot = loc_df.pivot_table(index="city", columns="job_category",
                                   values="count", fill_value=0).head(10)
        fig_heat = px.imshow(pivot, color_continuous_scale=["white", BLUE],
                             labels={"color": "공고 수"}, aspect="auto", height=260)
        fig_heat.update_layout(margin=dict(l=0, r=0, t=10, b=0), paper_bgcolor=WHITE,
                               coloraxis_colorbar=dict(thickness=10))
    else:
        fig_heat = empty_fig()

    return fig_co, fig_loc, fig_heat


# ── 실행 ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, port=8050)
