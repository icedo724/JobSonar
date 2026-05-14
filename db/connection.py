"""DB 연결 및 초기화 헬퍼."""
import re
import sqlite3
import os
from difflib import SequenceMatcher
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
        if "employment_type" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN employment_type TEXT")
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


def _normalize_title_for_dedup(s: str) -> str:
    """중복 감지용 제목 정규화.
    괄호·대괄호 내용 제거 → 소문자 → 한글·영숫자 이외 문자 모두 제거.
    결과: 공백 없는 연속 문자열 (예: "데이터엔지니어", "senoir데이터엔지니어")
    """
    s = s.lower()
    s = re.sub(r'[\(\[（【].*?[\)\]）】]', '', s)   # (경력 3년), [신입] 등 제거
    s = re.sub(r'[^a-z0-9가-힣ᄀ-ᇿ㄰-㆏]', '', s)
    return s


def _titles_are_duplicate(a: str, b: str) -> bool:
    """두 제목이 동일 공고를 나타낼 가능성이 높으면 True.

    판단 기준 (정규화 후):
    1. 짧은 쪽이 긴 쪽에 포함(contains) → 동일 직무에 접두/접미어가 붙은 케이스
       예: "데이터엔지니어" ⊂ "시니어데이터엔지니어"
    2. SequenceMatcher ratio ≥ 0.82 → 오탈자·띄어쓰기 차이 커버
       예: "데이터 엔지니어" vs "데이터엔지니어" → 정규화 후 동일(1.0)
    """
    na, nb = _normalize_title_for_dedup(a), _normalize_title_for_dedup(b)
    if not na or not nb:
        return False
    shorter, longer = (na, nb) if len(na) <= len(nb) else (nb, na)
    return shorter in longer or SequenceMatcher(None, na, nb).ratio() >= 0.82


def _is_cross_site_duplicate(conn: sqlite3.Connection, job: dict) -> bool:
    """동일 회사 + 유사 제목의 공고가 다른 사이트에 이미 존재하면 True.

    기존 완전 일치(lower+trim) → 퍼지 매칭(_titles_are_duplicate)으로 개선.
    같은 회사의 후보 공고를 모두 가져와 Python 레벨에서 유사도 비교.
    """
    rows = conn.execute(
        """
        SELECT title FROM jobs
        WHERE lower(trim(company_name)) = lower(trim(?))
          AND source_site               != ?
          AND is_active                 = 1
          AND is_duplicate              = 0
        """,
        (job["company_name"], job["source_site"]),
    ).fetchall()
    return any(_titles_are_duplicate(job["title"], row["title"]) for row in rows)


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
                industry, employment_type, location, experience_min, experience_max,
                salary_min, salary_max, posted_date, deadline_date, is_duplicate
            ) VALUES (
                :source_site, :source_id, :url, :title, :company_name, :job_category,
                :industry, :employment_type, :location, :experience_min, :experience_max,
                :salary_min, :salary_max, :posted_date, :deadline_date, :is_duplicate
            )
            """,
            {**job, "is_duplicate": int(is_dup),
             "industry": job.get("industry"), "employment_type": job.get("employment_type")},
        )
        return cur.lastrowid, "inserted"

    conn.execute(
        """
        UPDATE jobs
        SET title=:title, company_name=:company_name, is_active=1,
            industry=:industry, employment_type=:employment_type,
            salary_min=:salary_min, salary_max=:salary_max,
            deadline_date=:deadline_date, updated_at=CURRENT_TIMESTAMP
        WHERE source_site=:source_site AND source_id=:source_id
        """,
        {**job, "industry": job.get("industry"), "employment_type": job.get("employment_type")},
    )
    return existing["id"], "updated"


def insert_skills(conn: sqlite3.Connection, job_id: int, skills: list[str]) -> None:
    """스킬 태그 일괄 삽입 (중복 무시). normalize_skill 완료된 값 그대로 저장."""
    conn.executemany(
        "INSERT OR IGNORE INTO job_skills (job_id, skill_name) VALUES (?, ?)",
        [(job_id, s.strip()) for s in skills if s.strip()],
    )


def deactivate_expired_jobs(conn: sqlite3.Connection) -> dict[str, int]:
    """만료 공고를 is_active=0으로 표시.

    두 가지 기준으로 비활성화:
    1. deadline_date 기준: 마감일이 오늘 이전인 공고 (명시적 만료)
    2. 비활성 staleness 기준: 마감일 정보가 없고 7일 이상 크롤러에서 다시 발견되지 않은 공고
       - upsert_job()은 공고를 발견할 때마다 updated_at을 갱신하므로,
         updated_at이 오래된 공고 = 사이트에서 사라진 것으로 추정

    반환: {"by_deadline": N, "by_staleness": M}  (비활성화된 각 건수)
    """
    # 1) 마감일 지난 공고
    cur_deadline = conn.execute(
        """
        UPDATE jobs
        SET is_active = 0, updated_at = CURRENT_TIMESTAMP
        WHERE is_active = 1
          AND deadline_date IS NOT NULL
          AND deadline_date < date('now')
        """
    )
    # 2) 마감일 없이 7일 이상 미발견 공고
    cur_stale = conn.execute(
        """
        UPDATE jobs
        SET is_active = 0, updated_at = CURRENT_TIMESTAMP
        WHERE is_active = 1
          AND updated_at < datetime('now', '-7 days')
        """
    )
    return {
        "by_deadline": cur_deadline.rowcount,
        "by_staleness": cur_stale.rowcount,
    }
