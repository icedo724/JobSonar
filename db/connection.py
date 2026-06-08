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
        # 컬럼 마이그레이션 (기존 DB 호환)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(jobs)")]
        if "is_duplicate" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN is_duplicate BOOLEAN NOT NULL DEFAULT 0")
        if "industry" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN industry TEXT")
        if "employment_type" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN employment_type TEXT")
        if "description" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN description TEXT")
        if "duplicate_of" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN duplicate_of INTEGER")
        # duplicate_of 인덱스는 컬럼 마이그레이션 이후에 생성 (기존 DB 호환)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_dup_of ON jobs(duplicate_of)")
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


# 회사명 정규화 시 제거할 법인격·접미어 패턴
_COMPANY_NOISE = re.compile(
    r'\(주\)|\(유\)|\(재\)|\(사\)|㈜|㈐|주식회사|유한회사|유한책임회사|'
    r'\bco\.?,?\s*ltd\.?|\bltd\.?|\binc\.?|\bcorp\.?|\bcorporation\b|\bllc\b|\bgmbh\b',
    re.IGNORECASE,
)


def _normalize_company(s: str) -> str:
    """중복 감지용 회사명 정규화.
    법인격 표기((주)·주식회사·Inc·Co.,Ltd 등) 제거 → 소문자 → 공백·특수문자 제거.
    예: "(주)카카오" → "카카오", "Kakao Corp." → "kakao"
    """
    if not s:
        return ""
    s = _COMPANY_NOISE.sub(" ", s)
    s = s.lower()
    s = re.sub(r'[^a-z0-9가-힣]', '', s)
    return s


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


def _find_cross_site_duplicate(conn: sqlite3.Connection, job: dict) -> int | None:
    """다른 사이트에 동일 공고(같은 회사 + 유사 제목)가 이미 있으면 그 대표 공고 id 반환.

    회사명은 _normalize_company()로 정규화해 "(주)카카오" ↔ "카카오" 같은 표기 차이를 흡수하고,
    제목은 _titles_are_duplicate() 퍼지 매칭으로 비교한다.
    같은 회사로 좁힌 뒤 Python 레벨에서 유사도를 판정한다.
    """
    target_company = _normalize_company(job["company_name"])
    if not target_company:
        return None

    rows = conn.execute(
        """
        SELECT id, company_name, title FROM jobs
        WHERE source_site  != ?
          AND is_active    = 1
          AND is_duplicate = 0
        """,
        (job["source_site"],),
    ).fetchall()

    for row in rows:
        if _normalize_company(row["company_name"]) != target_company:
            continue
        if _titles_are_duplicate(job["title"], row["title"]):
            return row["id"]
    return None


def _is_cross_site_duplicate(conn: sqlite3.Connection, job: dict) -> bool:
    """다른 사이트에 동일 공고가 존재하는지 여부 (bool 래퍼)."""
    return _find_cross_site_duplicate(conn, job) is not None


def upsert_job(conn: sqlite3.Connection, job: dict) -> tuple[int, str]:
    """INSERT or UPDATE. Returns (job_id, 'inserted'|'updated')."""
    existing = conn.execute(
        "SELECT id FROM jobs WHERE source_site=? AND source_id=?",
        (job["source_site"], job["source_id"]),
    ).fetchone()

    if existing is None:
        canonical_id = _find_cross_site_duplicate(conn, job)
        cur = conn.execute(
            """
            INSERT INTO jobs (
                source_site, source_id, url, title, company_name, job_category,
                industry, employment_type, location, experience_min, experience_max,
                salary_min, salary_max, description, posted_date, deadline_date,
                is_duplicate, duplicate_of
            ) VALUES (
                :source_site, :source_id, :url, :title, :company_name, :job_category,
                :industry, :employment_type, :location, :experience_min, :experience_max,
                :salary_min, :salary_max, :description, :posted_date, :deadline_date,
                :is_duplicate, :duplicate_of
            )
            """,
            {**job,
             "industry": job.get("industry"), "employment_type": job.get("employment_type"),
             "description": job.get("description"),
             "is_duplicate": int(canonical_id is not None),
             "duplicate_of": canonical_id},
        )
        return cur.lastrowid, "inserted"

    conn.execute(
        """
        UPDATE jobs
        SET title=:title, company_name=:company_name, is_active=1,
            industry=:industry, employment_type=:employment_type,
            salary_min=:salary_min, salary_max=:salary_max,
            description=COALESCE(:description, description),
            deadline_date=:deadline_date, updated_at=CURRENT_TIMESTAMP
        WHERE source_site=:source_site AND source_id=:source_id
        """,
        {**job, "industry": job.get("industry"), "employment_type": job.get("employment_type"),
         "description": job.get("description")},
    )
    return existing["id"], "updated"


def insert_skills(conn: sqlite3.Connection, job_id: int, skills: list[str]) -> None:
    """스킬 태그 일괄 삽입 (중복 무시). normalize_skill 완료된 값 그대로 저장."""
    conn.executemany(
        "INSERT OR IGNORE INTO job_skills (job_id, skill_name) VALUES (?, ?)",
        [(job_id, s.strip()) for s in skills if s.strip()],
    )


def deactivate_unseen_jobs(
    conn: sqlite3.Connection,
    source_site: str,
    crawl_start_iso: str,
) -> int:
    """해당 소스에서 이번 크롤에 발견되지 않은 공고를 즉시 비활성화.

    upsert_job()은 공고를 발견할 때마다 updated_at을 현재 시각으로 갱신함.
    따라서 crawl_start_iso 이전에 updated_at이 머물러 있는 공고
    = 이번 크롤에서 한 번도 발견되지 않은 공고 = 사이트에서 내려진 것으로 판단.

    Args:
        source_site:    'wanted' | 'saramin' | 'jobkorea'
        crawl_start_iso: 크롤 시작 시각 (UTC, 'YYYY-MM-DD HH:MM:SS')

    반환: 비활성화된 공고 수
    """
    cur = conn.execute(
        """
        UPDATE jobs
        SET is_active = 0, updated_at = CURRENT_TIMESTAMP
        WHERE source_site = ?
          AND is_active   = 1
          AND updated_at  < ?
        """,
        (source_site, crawl_start_iso),
    )
    return cur.rowcount


def deactivate_expired_jobs(conn: sqlite3.Connection) -> int:
    """마감일이 지난 공고를 is_active=0으로 표시 (deadline_date 기준).

    당일 크롤 미발견 공고는 run_crawler() 내 deactivate_unseen_jobs()로 처리.
    이 함수는 deadline_date가 명시된 공고의 마감일 초과만 처리하는 보조 수단.

    반환: 비활성화된 공고 수
    """
    cur = conn.execute(
        """
        UPDATE jobs
        SET is_active = 0, updated_at = CURRENT_TIMESTAMP
        WHERE is_active = 1
          AND deadline_date IS NOT NULL
          AND deadline_date < date('now')
        """
    )
    return cur.rowcount
