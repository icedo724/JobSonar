"""대시보드 공통 헬퍼 함수 및 차트 스타일."""
import pandas as pd
import plotly.graph_objects as go
from dash import html

from analysis import normalize_location  # noqa: F401 — re-export for callbacks
from dashboard.context import BLUE_LIGHT, WHITE, GRAY


def apply_filter(
    df: pd.DataFrame,
    categories: list,
    sources: list,
    industries: list | None = None,
    emp_types: list | None = None,
) -> pd.DataFrame:
    if df.empty:
        return df
    mask = pd.Series(True, index=df.index)
    if categories and "job_category" in df.columns:
        mask &= df["job_category"].isin(categories)
    if sources and "source_site" in df.columns:
        mask &= df["source_site"].isin(sources)
    if industries and "industry" in df.columns:
        mask &= df["industry"].isin(industries)
    if emp_types and "employment_type" in df.columns:
        mask &= df["employment_type"].isin(emp_types)
    return df[mask]


def chart_base(fig: go.Figure, height: int = 380) -> go.Figure:
    fig.update_layout(
        height=height, plot_bgcolor=WHITE, paper_bgcolor=WHITE,
        margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        font=dict(family="Pretendard, Apple SD Gothic Neo, sans-serif"),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="#f0f0f0")
    return fig


def empty_fig(msg: str = "데이터 없음") -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(text=msg, x=0.5, y=0.5, xref="paper", yref="paper",
                       showarrow=False, font=dict(size=14, color=GRAY))
    fig.update_layout(height=300, plot_bgcolor=WHITE, paper_bgcolor=WHITE,
                      xaxis=dict(visible=False), yaxis=dict(visible=False))
    return fig


def section_wrap(title: str, *children):
    return html.Div(
        [html.P(title, className="section-title"), *children],
        className="chart-section",
    )


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


def exp_label(mn, mx) -> str:
    if mn is None or (isinstance(mn, float) and pd.isna(mn)):
        return "경력무관"
    mn = int(mn)
    mx = int(mx) if mx is not None and not (isinstance(mx, float) and pd.isna(mx)) else None
    if mn == 0 and mx == 0:
        return "신입"
    return f"{mn}~{mx}년" if mx else f"{mn}년 이상"


def salary_label(mn, mx) -> str:
    if mn is None or (isinstance(mn, float) and pd.isna(mn)):
        return "연봉 협의"
    mx_valid = mx is not None and not (isinstance(mx, float) and pd.isna(mx))
    return f"{int(mn):,}~{int(mx):,}만원" if mx_valid else f"{int(mn):,}만원~"
