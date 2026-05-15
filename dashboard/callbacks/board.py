"""공고 목록 탭 콜백."""
import math
import pandas as pd
from dash import Input, Output, State, html, ctx

from analysis import normalize_location
from dashboard.context import BOARD_DF
from dashboard.utils import apply_filter, source_badge, exp_label


def register(app) -> None:

    @app.callback(
        Output("board-location", "options"),
        Input("filter-categories", "value"),
        Input("filter-sources", "value"),
        Input("filter-industry", "value"),
        Input("filter-emp-type", "value"),
    )
    def update_location_options(categories, sources, industries, emp_types):
        df = apply_filter(BOARD_DF, categories, sources, industries, emp_types)
        locs = sorted({normalize_location(l) for l in df["location"].dropna()} - {""})
        return [{"label": l, "value": l} for l in locs]

    @app.callback(
        Output("board-page", "data"),
        Input("board-prev", "n_clicks"),
        Input("board-next", "n_clicks"),
        Input("board-search", "value"),
        Input("board-location", "value"),
        Input("board-exp", "value"),
        Input("board-sort", "value"),
        Input("filter-categories", "value"),
        Input("filter-sources", "value"),
        Input("filter-industry", "value"),
        Input("filter-emp-type", "value"),
        State("board-page", "data"),
    )
    def update_page(prev, nxt, search, location, exp, sort,
                    categories, sources, industries, emp_types, current):
        trigger = ctx.triggered_id
        if trigger in ("board-search", "board-location", "board-exp", "board-sort",
                       "filter-categories", "filter-sources", "filter-industry", "filter-emp-type"):
            return 1
        if trigger == "board-prev":
            return max(1, current - 1)
        if trigger == "board-next":
            return current + 1
        return 1

    @app.callback(
        Output("board-cards", "children"),
        Output("board-count", "children"),
        Output("board-page-info", "children"),
        Output("board-prev", "disabled"),
        Output("board-next", "disabled"),
        Input("filter-categories", "value"),
        Input("filter-sources", "value"),
        Input("filter-industry", "value"),
        Input("filter-emp-type", "value"),
        Input("board-search", "value"),
        Input("board-location", "value"),
        Input("board-exp", "value"),
        Input("board-sort", "value"),
        Input("board-page", "data"),
    )
    def update_board(categories, sources, industries, emp_types,
                     keyword, locations, exps, sort, page):
        PAGE_SIZE = 20
        if not categories or not sources:
            return [html.P("필터를 선택해 주세요.", className="no-data")], "총 0건", "1 / 1", True, True

        df = apply_filter(BOARD_DF, categories, sources, industries, emp_types)

        # 키워드 검색
        if keyword:
            kw = keyword.lower()
            df = df[
                df["title"].str.lower().str.contains(kw, na=False) |
                df["company_name"].str.lower().str.contains(kw, na=False) |
                df["skills"].fillna("").str.lower().str.contains(kw, na=False)
            ]

        # 지역 필터 (복수 선택)
        if locations:
            locs = locations if isinstance(locations, list) else [locations]
            df = df[df["location"].apply(normalize_location).isin(locs)]

        # 경력 필터 (복수 선택)
        if exps:
            exps = exps if isinstance(exps, list) else [exps]
            masks = []
            for e in exps:
                if e == "신입":
                    masks.append(df["experience_min"].fillna(-1) == 0)
                elif e == "경력":
                    masks.append(df["experience_min"].fillna(-1) > 0)
                elif e == "경력무관":
                    masks.append(df["experience_min"].isna())
            if masks:
                combined = masks[0]
                for m in masks[1:]:
                    combined |= m
                df = df[combined]

        # 정렬 (연봉 옵션 제거)
        sort_map = {
            "latest":   ("collected_at", False),
            "exp_asc":  ("experience_min", True),
            "exp_desc": ("experience_min", False),
        }
        col, asc = sort_map.get(sort or "latest", ("collected_at", False))
        df = df.sort_values(col, ascending=asc, na_position="last")

        total = len(df)
        total_pages = max(1, math.ceil(total / PAGE_SIZE))
        page = min(page, total_pages)
        page_data = df.iloc[(page - 1) * PAGE_SIZE: page * PAGE_SIZE]

        cards = []
        for _, row in page_data.iterrows():
            skills_el = html.Div(
                [html.Span(s.strip(), className="skill-badge")
                 for s in str(row.get("skills", "")).split("·")
                 if s.strip() and pd.notna(row.get("skills"))],
                className="job-skills",
            )

            # 메타: 지역 · 경력 (연봉 제거)
            meta_parts = [
                normalize_location(row.get("location")) or "",
                exp_label(row.get("experience_min"), row.get("experience_max")),
            ]
            meta = " · ".join(filter(None, meta_parts))

            cards.append(html.Div([
                html.Div([
                    html.Div([
                        html.A(row["title"], href=row["url"], target="_blank", className="job-title"),
                        html.P(row["company_name"], className="job-company"),
                    ]),
                    source_badge(row["source_site"]),
                ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "flex-start"}),
                html.P(meta, className="job-meta"),
                skills_el,
            ], className="job-card"))

        count_text = f"총 {total:,}건"
        page_info = f"{page} / {total_pages}"
        return (
            cards or [html.P("조건에 맞는 공고가 없습니다.", className="no-data")],
            count_text,
            page_info,
            page <= 1,
            page >= total_pages,
        )
