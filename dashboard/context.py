"""전역 데이터(로컬 SQLite) 및 색상 상수.

로컬 전용: 데이터는 로컬 data/jobsonar.db에서만 읽는다(외부 다운로드 없음).
대시보드의 '지금 갱신' 버튼이 크롤 후 reload_data()를 호출해 메모리 캐시를 새로 채운다.
콜백은 get_jobs_df()/get_board_df() 접근자로 호출 시점의 최신 데이터를 읽는다.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from db.connection import get_conn, init_db
from analysis import load_jobs_df, load_jobs_for_board

# ── 색상 상수 ─────────────────────────────────────────────────────
BLUE       = "#1352f1"
BLUE_LIGHT = "#e8f0fe"
WHITE      = "#ffffff"
GRAY       = "#6b7684"

# ── 고정 목록 ─────────────────────────────────────────────────────
ALL_SOURCES = ["wanted", "saramin", "jobkorea"]
EMP_TYPES   = ["정규직", "계약직", "인턴"]


class _DataCache:
    """크롤 갱신 시 통째로 교체되는 메모리 데이터 캐시."""
    jobs_df = None
    board_df = None
    categories: list = []
    industries: list = []
    last_updated: str = "—"


_cache = _DataCache()


def reload_data() -> _DataCache:
    """로컬 DB에서 데이터를 다시 읽어 캐시를 갱신. 크롤 직후 호출된다."""
    init_db()
    with get_conn() as conn:
        _cache.jobs_df = load_jobs_df(conn)
        _cache.board_df = load_jobs_for_board(conn)
    df = _cache.jobs_df
    _cache.categories = sorted(df["job_category"].dropna().unique().tolist())
    _cache.industries = (
        sorted(df["industry"].dropna().unique().tolist())
        if "industry" in df.columns else []
    )
    if not df.empty and "collected_at" in df.columns and df["collected_at"].notna().any():
        # collected_at은 SQLite CURRENT_TIMESTAMP(UTC) → KST(+9h)로 표시
        import pandas as pd
        kst = df["collected_at"].max() + pd.Timedelta(hours=9)
        _cache.last_updated = kst.strftime("%Y-%m-%d %H:%M")
    return _cache


def get_jobs_df():
    """호출 시점의 최신 전체 공고 DataFrame."""
    return _cache.jobs_df


def get_board_df():
    """호출 시점의 최신 공고목록(보드) DataFrame."""
    return _cache.board_df


def get_last_updated() -> str:
    return _cache.last_updated


# ── 최초 로드 (앱 import 시 1회) ──────────────────────────────────
reload_data()

# 레이아웃은 import 시 1회만 구성되므로 초기 스냅샷을 노출 (직군은 고정 4종)
ALL_CATEGORIES = _cache.categories
ALL_INDUSTRIES = _cache.industries
HAS_DATA       = _cache.jobs_df is not None and len(_cache.jobs_df) > 0
