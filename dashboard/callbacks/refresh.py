"""'지금 갱신' 버튼 콜백 — 로컬에서 즉시 크롤 후 데이터 캐시 재로드."""
import logging

from dash import Input, Output, State, html, no_update

from crawler.run import run_all
from dashboard.context import reload_data, get_last_updated

logger = logging.getLogger(__name__)

# 버튼 1회 갱신 시 사이트당 수집 페이지 수 (속도/완성도 균형, 필요시 조정)
REFRESH_MAX_PAGES = 8


def register(app) -> None:

    @app.callback(
        Output("data-version", "data"),
        Output("refresh-status", "children"),
        Input("refresh-btn", "n_clicks"),
        State("data-version", "data"),
        prevent_initial_call=True,
    )
    def do_refresh(n_clicks, version):
        if not n_clicks:
            return no_update, no_update
        try:
            result = run_all(max_pages=REFRESH_MAX_PAGES)
            reload_data()
            t = result["totals"]
            msg = (
                f"갱신 완료 · 신규 {t['inserted']} · 업데이트 {t['updated']} · "
                f"마지막 {get_last_updated()}"
            )
            return (version or 0) + 1, html.Span(msg, className="refresh-ok")
        except Exception as e:  # noqa: BLE001
            logger.exception("갱신 실패")
            return no_update, html.Span(f"갱신 실패: {e}", className="refresh-err")
