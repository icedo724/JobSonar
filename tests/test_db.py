"""DB 연결 및 upsert 단위 테스트 (인메모리 SQLite 사용)."""
import sys
import sqlite3
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from unittest.mock import MagicMock
from db.connection import (
    init_db, get_conn, upsert_job, insert_skills,
    _is_cross_site_duplicate, _titles_are_duplicate,
    deactivate_expired_jobs,
)
from crawler.run import validate_job_links

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


# ── _titles_are_duplicate (퍼지 매칭) ─────────────────────────────

class TestTitlesAreDuplicate:
    def test_exact_same(self):
        assert _titles_are_duplicate("데이터 엔지니어", "데이터 엔지니어") is True

    def test_space_difference(self):
        """띄어쓰기 차이는 동일 공고로 처리."""
        assert _titles_are_duplicate("데이터 엔지니어", "데이터엔지니어") is True

    def test_bracket_suffix_ignored(self):
        """괄호 안 내용 제거 후 비교: (경력), [신입] 등은 무시."""
        assert _titles_are_duplicate("데이터 엔지니어 (경력 3년↑)", "데이터 엔지니어") is True
        assert _titles_are_duplicate("데이터 분석가 [신입]", "데이터 분석가") is True

    def test_senior_prefix(self):
        """직급 접두어 포함 → 짧은 쪽이 긴 쪽에 포함."""
        assert _titles_are_duplicate("시니어 데이터 엔지니어", "데이터 엔지니어") is True
        assert _titles_are_duplicate("Senior 데이터 엔지니어", "데이터 엔지니어") is True

    def test_different_job_type(self):
        """완전히 다른 직군은 중복 아님."""
        assert _titles_are_duplicate("데이터 엔지니어", "데이터 분석가") is False

    def test_ml_vs_data_engineer(self):
        assert _titles_are_duplicate("ML 엔지니어", "데이터 엔지니어") is False

    def test_empty_strings(self):
        assert _titles_are_duplicate("", "데이터 엔지니어") is False
        assert _titles_are_duplicate("데이터 엔지니어", "") is False


class TestFuzzyCrossSiteDuplicate:
    def test_fuzzy_detects_space_diff(self):
        """사이트마다 띄어쓰기가 달라도 중복으로 식별."""
        conn = _make_in_memory_conn()
        upsert_job(conn, _sample_job(source_site="wanted", source_id="1", title="데이터 엔지니어"))
        is_dup = _is_cross_site_duplicate(
            conn,
            _sample_job(source_site="saramin", source_id="99", title="데이터엔지니어"),
        )
        assert is_dup is True

    def test_fuzzy_detects_bracket_diff(self):
        """괄호로 경력 표기 차이도 중복으로 식별."""
        conn = _make_in_memory_conn()
        upsert_job(conn, _sample_job(source_site="wanted", source_id="1", title="데이터 엔지니어"))
        is_dup = _is_cross_site_duplicate(
            conn,
            _sample_job(source_site="jobkorea", source_id="99", title="데이터 엔지니어 (경력 5년↑)"),
        )
        assert is_dup is True

    def test_fuzzy_does_not_false_positive(self):
        """다른 직군은 중복으로 처리하지 않음."""
        conn = _make_in_memory_conn()
        upsert_job(conn, _sample_job(source_site="wanted", source_id="1", title="데이터 엔지니어"))
        is_dup = _is_cross_site_duplicate(
            conn,
            _sample_job(source_site="saramin", source_id="99", title="데이터 분석가"),
        )
        assert is_dup is False


# ── validate_job_links ────────────────────────────────────────────

def _mock_head(status_code: int):
    """requests.Session.head() 를 흉내내는 mock 반환."""
    resp = MagicMock()
    resp.status_code = status_code
    session = MagicMock()
    session.head.return_value = resp
    return session


class TestValidateJobLinks:
    def test_deactivates_404_link(self):
        """HTTP 404 응답 → is_active=0."""
        conn = _make_in_memory_conn()
        job_id, _ = upsert_job(conn, _sample_job(source_id="10", deadline_date=None))
        conn.commit()

        result = validate_job_links(conn, session=_mock_head(404), delay=0)
        conn.commit()

        assert result["deactivated"] == 1
        row = conn.execute("SELECT is_active FROM jobs WHERE id=?", (job_id,)).fetchone()
        assert row["is_active"] == 0

    def test_keeps_200_active(self):
        """HTTP 200 응답 → is_active 유지."""
        conn = _make_in_memory_conn()
        job_id, _ = upsert_job(conn, _sample_job(source_id="11", deadline_date=None))
        conn.commit()

        result = validate_job_links(conn, session=_mock_head(200), delay=0)
        conn.commit()

        assert result["deactivated"] == 0
        row = conn.execute("SELECT is_active FROM jobs WHERE id=?", (job_id,)).fetchone()
        assert row["is_active"] == 1

    def test_skips_jobs_with_deadline(self):
        """마감일이 설정된 공고는 URL 검사 대상 제외 (deadline 기반 비활성화와 역할 분리)."""
        conn = _make_in_memory_conn()
        upsert_job(conn, _sample_job(source_id="12", deadline_date="2099-12-31"))
        conn.commit()

        result = validate_job_links(conn, session=_mock_head(404), delay=0)
        assert result["checked"] == 0

    def test_network_error_ignored(self):
        """네트워크 오류 발생 시 해당 공고는 건너뜀 (보수적 접근)."""
        conn = _make_in_memory_conn()
        job_id, _ = upsert_job(conn, _sample_job(source_id="13", deadline_date=None))
        conn.commit()

        session = MagicMock()
        session.head.side_effect = Exception("connection timeout")

        result = validate_job_links(conn, session=session, delay=0)
        conn.commit()

        assert result["deactivated"] == 0
        row = conn.execute("SELECT is_active FROM jobs WHERE id=?", (job_id,)).fetchone()
        assert row["is_active"] == 1

    def test_returns_correct_keys(self):
        conn = _make_in_memory_conn()
        result = validate_job_links(conn, session=_mock_head(200), delay=0)
        assert "checked" in result
        assert "deactivated" in result


# ── deactivate_expired_jobs ───────────────────────────────────────

class TestDeactivateExpiredJobs:
    def test_deactivates_past_deadline(self):
        """마감일이 지난 공고는 비활성화."""
        conn = _make_in_memory_conn()
        upsert_job(conn, _sample_job(source_id="1", deadline_date="2020-01-01"))
        conn.commit()
        result = deactivate_expired_jobs(conn)
        conn.commit()
        assert result["by_deadline"] >= 1
        row = conn.execute("SELECT is_active FROM jobs WHERE source_id='1'").fetchone()
        assert row["is_active"] == 0

    def test_keeps_future_deadline_active(self):
        """마감일이 아직 남은 공고는 유지."""
        conn = _make_in_memory_conn()
        upsert_job(conn, _sample_job(source_id="2", deadline_date="2099-12-31"))
        conn.commit()
        deactivate_expired_jobs(conn)
        conn.commit()
        row = conn.execute("SELECT is_active FROM jobs WHERE source_id='2'").fetchone()
        assert row["is_active"] == 1

    def test_deactivates_stale_no_deadline(self):
        """마감일 없이 7일 이상 발견되지 않은 공고는 비활성화."""
        conn = _make_in_memory_conn()
        upsert_job(conn, _sample_job(source_id="3", deadline_date=None))
        conn.commit()
        # updated_at을 8일 전으로 강제 설정
        conn.execute(
            "UPDATE jobs SET updated_at = datetime('now', '-8 days') WHERE source_id='3'"
        )
        conn.commit()
        result = deactivate_expired_jobs(conn)
        conn.commit()
        assert result["by_staleness"] >= 1
        row = conn.execute("SELECT is_active FROM jobs WHERE source_id='3'").fetchone()
        assert row["is_active"] == 0

    def test_keeps_recent_no_deadline_active(self):
        """마감일 없어도 최근(1일)에 발견된 공고는 유지."""
        conn = _make_in_memory_conn()
        upsert_job(conn, _sample_job(source_id="4", deadline_date=None))
        conn.commit()
        deactivate_expired_jobs(conn)
        conn.commit()
        row = conn.execute("SELECT is_active FROM jobs WHERE source_id='4'").fetchone()
        assert row["is_active"] == 1

    def test_already_inactive_not_double_counted(self):
        """이미 비활성인 공고는 카운트에 포함되지 않음 (rowcount=0)."""
        conn = _make_in_memory_conn()
        upsert_job(conn, _sample_job(source_id="5", deadline_date="2020-01-01"))
        conn.execute("UPDATE jobs SET is_active=0 WHERE source_id='5'")
        conn.commit()
        result = deactivate_expired_jobs(conn)
        # 이미 0인 행은 UPDATE 영향 없음
        assert result["by_deadline"] == 0
        assert result["by_staleness"] == 0

    def test_returns_correct_counts(self):
        """반환 딕셔너리에 by_deadline / by_staleness 키가 있음."""
        conn = _make_in_memory_conn()
        result = deactivate_expired_jobs(conn)
        assert "by_deadline" in result
        assert "by_staleness" in result
