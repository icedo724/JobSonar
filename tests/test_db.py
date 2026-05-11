"""DB 연결 및 upsert 단위 테스트 (인메모리 SQLite 사용)."""
import sys
import sqlite3
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from db.connection import init_db, get_conn, upsert_job, insert_skills, _is_cross_site_duplicate

# 테스트용 인메모리 DB 경로
_TEST_DB = Path(":memory:")


def _make_in_memory_conn():
    """인메모리 SQLite 커넥션 생성 후 스키마 초기화."""
    schema = (Path(__file__).parent.parent / "db" / "schema.sql").read_text(encoding="utf-8")
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(schema)
    return conn


def _sample_job(**overrides) -> dict:
    base = {
        "source_site": "wanted",
        "source_id":   "12345",
        "url":         "https://wanted.co.kr/wd/12345",
        "title":       "데이터 엔지니어",
        "company_name": "테스트컴퍼니",
        "job_category": "데이터 엔지니어",
        "industry":     "IT",
        "employment_type": "정규직",
        "location":     "서울",
        "experience_min": 3,
        "experience_max": 7,
        "salary_min":   5000,
        "salary_max":   8000,
        "posted_date":  "2025-01-01",
        "deadline_date": "2025-03-31",
    }
    base.update(overrides)
    return base


# ── upsert_job ────────────────────────────────────────────────────

class TestUpsertJob:
    def test_insert_new_job(self):
        conn = _make_in_memory_conn()
        job_id, action = upsert_job(conn, _sample_job())
        assert action == "inserted"
        assert job_id > 0

    def test_update_existing_job(self):
        conn = _make_in_memory_conn()
        upsert_job(conn, _sample_job())
        _, action = upsert_job(conn, _sample_job(title="변경된 제목"))
        assert action == "updated"

    def test_unique_key_is_source_and_id(self):
        conn = _make_in_memory_conn()
        upsert_job(conn, _sample_job(source_site="wanted",  source_id="1"))
        upsert_job(conn, _sample_job(source_site="saramin", source_id="1"))
        count = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        assert count == 2  # 사이트가 다르면 별개 공고

    def test_updated_job_is_active(self):
        conn = _make_in_memory_conn()
        upsert_job(conn, _sample_job())
        # is_active=0으로 수동 설정 후 upsert → 다시 1로 복원
        conn.execute("UPDATE jobs SET is_active=0 WHERE source_site='wanted' AND source_id='12345'")
        upsert_job(conn, _sample_job(title="업데이트됨"))
        row = conn.execute("SELECT is_active FROM jobs WHERE source_site='wanted'").fetchone()
        assert row["is_active"] == 1


# ── insert_skills ─────────────────────────────────────────────────

class TestInsertSkills:
    def test_insert_skills(self):
        conn = _make_in_memory_conn()
        job_id, _ = upsert_job(conn, _sample_job())
        insert_skills(conn, job_id, ["Python", "SQL", "Apache Spark"])
        conn.commit()
        rows = conn.execute("SELECT skill_name FROM job_skills WHERE job_id=?", (job_id,)).fetchall()
        names = {r["skill_name"] for r in rows}
        assert names == {"Python", "SQL", "Apache Spark"}

    def test_preserves_canonical_case(self):
        """소문자로 저장되지 않고 canonical 표기 그대로 저장."""
        conn = _make_in_memory_conn()
        job_id, _ = upsert_job(conn, _sample_job())
        insert_skills(conn, job_id, ["Python", "PostgreSQL", "Apache Spark"])
        conn.commit()
        rows = conn.execute("SELECT skill_name FROM job_skills WHERE job_id=?", (job_id,)).fetchall()
        names = {r["skill_name"] for r in rows}
        # 소문자가 아닌 canonical 값으로 저장되어야 함
        assert "Python" in names
        assert "python" not in names
        assert "PostgreSQL" in names
        assert "Apache Spark" in names

    def test_no_duplicate_skills(self):
        conn = _make_in_memory_conn()
        job_id, _ = upsert_job(conn, _sample_job())
        insert_skills(conn, job_id, ["Python", "Python", "Python"])
        conn.commit()
        count = conn.execute(
            "SELECT COUNT(*) FROM job_skills WHERE job_id=? AND skill_name='Python'",
            (job_id,),
        ).fetchone()[0]
        assert count == 1

    def test_empty_skills_no_error(self):
        conn = _make_in_memory_conn()
        job_id, _ = upsert_job(conn, _sample_job())
        insert_skills(conn, job_id, [])
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM job_skills WHERE job_id=?", (job_id,)).fetchone()[0]
        assert count == 0


# ── _is_cross_site_duplicate ──────────────────────────────────────

class TestCrossSiteDuplicate:
    def test_detects_duplicate(self):
        conn = _make_in_memory_conn()
        upsert_job(conn, _sample_job(source_site="wanted", source_id="1"))
        # 같은 회사+제목이 다른 사이트에 있으면 중복
        is_dup = _is_cross_site_duplicate(
            conn,
            _sample_job(source_site="saramin", source_id="99"),
        )
        assert is_dup is True

    def test_not_duplicate_different_title(self):
        conn = _make_in_memory_conn()
        upsert_job(conn, _sample_job(source_site="wanted", source_id="1"))
        is_dup = _is_cross_site_duplicate(
            conn,
            _sample_job(source_site="saramin", source_id="99", title="전혀 다른 공고"),
        )
        assert is_dup is False

    def test_not_duplicate_same_site(self):
        conn = _make_in_memory_conn()
        upsert_job(conn, _sample_job(source_site="wanted", source_id="1"))
        is_dup = _is_cross_site_duplicate(
            conn,
            _sample_job(source_site="wanted", source_id="2"),
        )
        assert is_dup is False
