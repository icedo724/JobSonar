"""JobSonar — Dash 대시보드. 실행: python dashboard/app.py"""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dash import Dash, dcc, html

# context 임포트가 DB 로드를 트리거함 (앱 시작 시 1회)
from dashboard.context import (
    ALL_CATEGORIES, ALL_SOURCES, ALL_INDUSTRIES, EMP_TYPES,
    BLUE, BLUE_LIGHT, WHITE, GRAY,
)
from dashboard.utils import section_wrap
import dashboard.callbacks as callbacks

# ── 탭 스타일 ─────────────────────────────────────────────────────
_TAB = dict(
    padding="10px 20px", color=GRAY, fontWeight=500,
    fontSize="0.9rem", background=WHITE, borderBottom="2px solid transparent",
)
_TAB_SEL = {
    **_TAB,
    "color": BLUE, "borderBottom": f"2px solid {BLUE}",
    "fontWeight": 700, "background": BLUE_LIGHT,
}

# ── 앱 초기화 ─────────────────────────────────────────────────────
app = Dash(__name__, assets_folder="assets", suppress_callback_exceptions=True)
server = app.server  # gunicorn 진입점

# ── 레이아웃 ─────────────────────────────────────────────────────
app.layout = html.Div([

    # 사이드바
    html.Div([
        html.Div([
            html.Span("JobSonar", style={"fontSize": "1.15rem", "fontWeight": 800, "color": BLUE}),
        ], style={"marginBottom": "4px"}),
        html.P("데이터직군 공고 모음", className="sidebar-sub"),

        html.Hr(className="sidebar-hr"),
        html.Label("직군", className="filter-label"),
        dcc.Checklist(
            id="filter-categories",
            options=[{"label": c, "value": c} for c in ALL_CATEGORIES],
            value=ALL_CATEGORIES,
            className="pill-checklist",
            inputStyle={"display": "none"},
            labelStyle={"display": "inline-block"},
        ),

        html.Hr(className="sidebar-hr"),
        html.Label("플랫폼", className="filter-label"),
        dcc.Checklist(
            id="filter-sources",
            options=[
                {"label": "원티드",   "value": "wanted"},
                {"label": "사람인",   "value": "saramin"},
                {"label": "잡코리아", "value": "jobkorea"},
            ],
            value=ALL_SOURCES,
            className="pill-checklist",
            inputStyle={"display": "none"},
            labelStyle={"display": "inline-block"},
        ),

        html.Hr(className="sidebar-hr"),
        html.Label("근무형태", className="filter-label"),
        dcc.Checklist(
            id="filter-emp-type",
            options=[{"label": e, "value": e} for e in EMP_TYPES],
            value=[],
            className="pill-checklist",
            inputStyle={"display": "none"},
            labelStyle={"display": "inline-block"},
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
            html.H1("JobSonar"),
            html.P("데이터직군 공고 모음 · 원티드 · 사람인 · 잡코리아"),
        ], className="header-banner"),

        # KPI 카드
        html.Div(id="kpi-row", className="kpi-row"),

        # 탭
        dcc.Tabs(id="main-tabs", value="board", className="tabs-container", children=[

            # ── 공고 목록 ─────────────────────────────────────────
            dcc.Tab(label="공고 목록", value="board", style=_TAB, selected_style=_TAB_SEL, children=[
                html.Div([
                    html.Div([
                        dcc.Input(
                            id="board-search", placeholder="공고명 / 회사명 / 기술스택",
                            type="text", debounce=True, className="search-input",
                        ),
                        dcc.Dropdown(
                            id="board-location", placeholder="지역", clearable=True,
                            className="filter-dropdown",
                        ),
                        dcc.Dropdown(
                            id="board-exp", placeholder="경력", clearable=True,
                            options=["신입", "경력", "경력무관"],
                            className="filter-dropdown",
                        ),
                        dcc.Dropdown(
                            id="board-sort", placeholder="정렬", clearable=False,
                            value="latest",
                            options=[
                                {"label": "최신순",      "value": "latest"},
                                {"label": "경력 낮은순", "value": "exp_asc"},
                                {"label": "경력 높은순", "value": "exp_desc"},
                                {"label": "연봉 높은순", "value": "salary_desc"},
                            ],
                            className="filter-dropdown",
                        ),
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
            dcc.Tab(label="트렌드", value="trend", style=_TAB, selected_style=_TAB_SEL, children=[
                html.Div(id="trend-content", className="tab-inner"),
            ]),

            # ── 기술 스택 ─────────────────────────────────────────
            dcc.Tab(label="기술 스택", value="skills", style=_TAB, selected_style=_TAB_SEL, children=[
                html.Div([
                    html.Div([
                        html.Div([
                            section_wrap(
                                "직군별 요구 기술 TOP N",
                                html.Div([
                                    dcc.Dropdown(
                                        id="skill-cat", placeholder="직군 선택",
                                        className="filter-dropdown",
                                        style={"marginBottom": "12px"},
                                    ),
                                    dcc.Slider(
                                        id="skill-top-n", min=5, max=30, step=5, value=20,
                                        marks={5:"5", 10:"10", 15:"15", 20:"20", 25:"25", 30:"30"},
                                    ),
                                    dcc.Graph(id="skill-bar-graph"),
                                ]),
                            ),
                        ], style={"flex": "3"}),
                        html.Div([
                            section_wrap("스킬 급상승 (최근 2주)", dcc.Graph(id="skill-growth-graph")),
                            section_wrap("전체 스킬 순위", html.Div(id="skill-rank-table")),
                        ], style={"flex": "2"}),
                    ], style={"display": "flex", "gap": "16px"}),
                ], className="tab-inner"),
            ]),

            # ── 스킬 네트워크 ─────────────────────────────────────
            dcc.Tab(label="스킬 네트워크", value="network", style=_TAB, selected_style=_TAB_SEL, children=[
                html.Div([
                    html.Div([
                        dcc.Dropdown(
                            id="net-category", placeholder="직군 (전체)", clearable=True,
                            options=[{"label": c, "value": c} for c in ALL_CATEGORIES],
                            className="filter-dropdown",
                            style={"width": "240px"},
                        ),
                        html.Div([
                            html.Label(
                                "최소 공동 출현 횟수",
                                style={"fontSize": "0.83rem", "color": GRAY, "marginBottom": "4px"},
                            ),
                            dcc.Slider(
                                id="net-min-cooccur", min=2, max=20, step=1, value=3,
                                marks={2:"2", 5:"5", 10:"10", 15:"15", 20:"20"},
                            ),
                        ], style={"flex": "1"}),
                    ], style={"display": "flex", "gap": "20px", "alignItems": "flex-end", "marginBottom": "14px"}),
                    html.Div(id="network-metrics",
                             style={"display": "flex", "gap": "16px", "marginBottom": "12px"}),
                    dcc.Graph(id="network-graph"),
                ], className="tab-inner"),
            ]),

            # ── 연봉 분석 ─────────────────────────────────────────
            dcc.Tab(label="연봉 분석", value="salary", style=_TAB, selected_style=_TAB_SEL, children=[
                html.Div(id="salary-content", className="tab-inner"),
            ]),

            # ── 기업 분석 ─────────────────────────────────────────
            dcc.Tab(label="기업 분석", value="company", style=_TAB, selected_style=_TAB_SEL, children=[
                html.Div([
                    html.Div([
                        html.Div([
                            section_wrap(
                                "채용 공고 상위 기업",
                                html.Div([
                                    dcc.Slider(
                                        id="company-top-n", min=10, max=40, step=5, value=20,
                                        marks={10:"10", 20:"20", 30:"30", 40:"40"},
                                    ),
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

# ── 콜백 등록 ─────────────────────────────────────────────────────
callbacks.register_all(app)

# ── 실행 ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, port=8050)
