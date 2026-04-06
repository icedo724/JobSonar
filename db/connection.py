"""DB 연결 및 초기화 헬퍼."""
import sqlite3
import os
from pathlib import Path
from contextlib import contextmanager

DB_PATH = Path(__file__).parent.parent / "data" / "jobsonar.db"
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def init_db(db_path: Path = DB_PATH) -> None:
    """DB 파일 생성, 스키마 초기화, 마이그레이션."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        # is_duplicate 컬럼 마이그레이션 (기존 DB 호환)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(jobs)")]
        if "is_duplicate" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN is_duplicate BOOLEAN NOT NULL DEFAULT 0")
        if "industry" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN industry TEXT")
        conn.commit()


@contextmanager
def get_conn(db_path: Path = DB_PATH):
    """SQLite 커넥션 컨텍스트 매니저."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # dict-like 접근
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")  # 동시 읽기 성능
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _is_cross_site_duplicate(conn: sqlite3.Connection, job: dict) -> bool:
    """동일 회사+제목의 공고가 다른 사이트에 이미 존재하면 True."""
    row = conn.execute(
        """
        SELECT 1 FROM jobs
        WHERE lower(trim(company_name)) = lower(trim(?))
          AND lower(trim(title))        = lower(trim(?))
          AND source_site               != ?
          AND is_active                 = 1
          AND is_duplicate              = 0
        LIMIT 1
        """,
        (job["company_name"], job["title"], job["source_site"]),
    ).fetchone()
    return row is not None


def upsert_job(conn: sqlite3.Connection, job: dict) -> tuple[int, str]:
    """INSERT or UPDATE. Returns (job_id, 'inserted'|'updated')."""
    existing = conn.execute(
        "SELECT id FROM jobs WHERE source_site=? AND source_id=?",
        (job["source_site"], job["source_id"]),
    ).fetchone()

    if existing is None:
        is_dup = _is_cross_site_duplicate(conn, job)
        cur = conn.execute(
            """
            INSERT INTO jobs (
                source_site, source_id, url, title, company_name, job_category,
                industry, location, experience_min, experience_max,
                salary_min, salary_max, posted_date, deadline_date, is_duplicate
            ) VALUES (
                :source_site, :source_id, :url, :title, :company_name, :job_category,
                :industry, :location, :experience_min, :experience_max,
                :salary_min, :salary_max, :posted_date, :deadline_date, :is_duplicate
            )
            """,
            {**job, "is_duplicate": int(is_dup), "industry": job.get("industry")},
        )
        return cur.lastrowid, "inserted"

    conn.execute(
        """
        UPDATE jobs
        SET title=:title, company_name=:company_name, is_active=1,
            industry=:industry, salary_min=:salary_min, salary_max=:salary_max,
            deadline_date=:deadline_date, updated_at=CURRENT_TIMESTAMP
        WHERE source_site=:source_site AND source_id=:source_id
        """,
        {**job, "industry": job.get("industry")},
    )
    return existing["id"], "updated"


def insert_skills(conn: sqlite3.Connection, job_id: int, skills: list[str]) -> None:
    """스킬 태그 일괄 삽입 (중복 무시)."""
    conn.executemany(
        "INSERT OR IGNORE INTO job_skills (job_id, skill_name) VALUES (?, ?)",
        [(job_id, s.lower().strip()) for s in skills if s.strip()],
    )
