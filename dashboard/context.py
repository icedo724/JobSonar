"""앱 시작 시 1회 로드되는 전역 데이터 및 색상 상수."""
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from db.connection import get_conn, init_db, DB_PATH
from analysis import load_jobs_df, load_skills_df, load_jobs_for_board

# ── 색상 상수 ─────────────────────────────────────────────────────
BLUE       = "#1352f1"
BLUE_LIGHT = "#e8f0fe"
WHITE      = "#ffffff"
GRAY       = "#6b7684"
PALETTE    = ["#1352f1", "#4f7ef7", "#0abf7a", "#f5a623", "#e83e3e"]

# ── 고정 목록 ─────────────────────────────────────────────────────
ALL_SOURCES = ["wanted", "saramin", "jobkorea"]
EMP_TYPES   = ["정규직", "계약직", "인턴"]

HF_DATASET_REPO = os.getenv("HF_DATASET_REPO", "")


def _ensure_db() -> None:
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


_ensure_db()
init_db()

with get_conn() as _conn:
    JOBS_DF   = load_jobs_df(_conn)
    SKILLS_DF = load_skills_df(_conn)
    BOARD_DF  = load_jobs_for_board(_conn)

ALL_CATEGORIES = sorted(JOBS_DF["job_category"].dropna().unique().tolist())
HAS_DATA       = len(JOBS_DF) > 0
ALL_INDUSTRIES = (
    sorted(JOBS_DF["industry"].dropna().unique().tolist())
    if "industry" in JOBS_DF.columns else []
)
