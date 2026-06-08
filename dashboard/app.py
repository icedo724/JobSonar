"""JobSonar — Dash 대시보드. 실행: python dashboard/app.py"""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dash import Dash, dcc, html

from dashboard.context import (
    ALL_CATEGORIES, ALL_SOURCES, ALL_INDUSTRIES, EMP_TYPES,
    BLUE, BLUE_LIGHT, WHITE, GRAY,
)
import dashboard.callbacks as callbacks

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
        html.P("로컬 전용 · '지금 갱신'으로 수집", className="sidebar-caption"),
    ], className="sidebar"),

    # 전역: 데이터 버전 (갱신 버튼이 증가시키면 데이터 소비 콜백이 재실행됨)
    dcc.Store(id="data-version", data=0),

    # 메인 영역
    html.Div([
        # 헤더 배너
        html.Div([
            html.Div([
                html.H1("JobSonar"),
                html.P("데이터직군 공고 모음 · 원티드 · 사람인 · 잡코리아"),
            ]),
            html.Div([
                html.Button("지금 갱신", id="refresh-btn", n_clicks=0, className="refresh-btn"),
                dcc.Loading(
                    html.Div(id="refresh-status", className="refresh-status"),
                    type="circle", color=WHITE,
                ),
            ], className="refresh-wrap"),
        ], className="header-banner"),

        # KPI 카드
        html.Div(id="kpi-row", className="kpi-row"),

        # 공고 목록
        html.Div([
            html.Div([
                dcc.Input(
                    id="board-search", placeholder="공고명 / 회사명 / 기술스택",
                    type="text", debounce=True, className="search-input",
                ),
                dcc.Dropdown(
                    id="board-location", placeholder="지역 (복수 선택 가능)", clearable=True,
                    multi=True,
                    className="filter-dropdown",
                ),
                dcc.Dropdown(
                    id="board-exp", placeholder="경력 (복수 선택 가능)", clearable=True,
                    multi=True,
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

    ], className="main"),
], className="app-wrapper")

# ── 콜백 등록 ─────────────────────────────────────────────────────
callbacks.register_all(app)

# ── 실행 ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, port=8050)
