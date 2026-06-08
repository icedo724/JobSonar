"""크롤러 파서 단위 테스트."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from crawler.base import normalize_skill, extract_skills_from_text, SKILL_ALIASES, is_relevant_job
from crawler.wanted import _parse_salary, _parse_experience
from crawler.saramin import _parse_salary_saramin, SaraminCrawler
from crawler.jobkorea import _parse_experience_jk, _parse_deadline_jk, _normalize_employment_jk


# ── normalize_skill ───────────────────────────────────────────────

class TestNormalizeSkill:
    def test_known_alias_lowercase(self):
        assert normalize_skill("python") == "Python"

    def test_known_alias_with_spaces(self):
        assert normalize_skill("apache spark") == "Apache Spark"

    def test_unknown_passthrough(self):
        assert normalize_skill("SomeWeirdTech") == "SomeWeirdTech"

    def test_strips_whitespace(self):
        assert normalize_skill("  python  ") == "Python"

    def test_korean_alias(self):
        assert normalize_skill("파이썬") == "Python"

    def test_github_separate_from_git(self):
        """GitHub은 Git과 별도 스킬로 추적."""
        assert normalize_skill("github") == "GitHub"
        assert normalize_skill("gitlab") == "GitLab"
        assert normalize_skill("git") == "Git"

    def test_case_insensitive(self):
        # normalize_skill은 내부에서 .lower() 처리 → 대소문자 무관하게 canonical 값 반환
        assert normalize_skill("PYTHON") == "Python"
        assert normalize_skill("Python") == "Python"
        assert normalize_skill("python") == "Python"


# ── extract_skills_from_text ──────────────────────────────────────

class TestExtractSkillsFromText:
    def test_basic_extraction(self):
        skills = extract_skills_from_text("Python, SQL, AWS 경험 우대")
        assert "Python" in skills
        assert "SQL" in skills
        assert "AWS" in skills

    def test_case_insensitive_text(self):
        skills = extract_skills_from_text("PYTHON and postgresql required")
        assert "Python" in skills
        assert "PostgreSQL" in skills

    def test_short_keyword_word_boundary(self):
        # 'r' 은 단어 경계에서만 매핑
        skills_with = extract_skills_from_text("R 프로그래밍 필수")
        skills_without = extract_skills_from_text("architecture")
        assert "R" in skills_with
        assert "R" not in skills_without

    def test_returns_sorted_list(self):
        skills = extract_skills_from_text("Spark Python SQL")
        assert skills == sorted(skills)

    def test_no_duplicate_canonical(self):
        # pyspark와 spark는 둘 다 Apache Spark로 정규화 → 중복 없어야 함
        skills = extract_skills_from_text("spark pyspark apache spark")
        assert skills.count("Apache Spark") == 1

    def test_empty_text(self):
        assert extract_skills_from_text("") == []


# ── is_relevant_job (관련성 필터) ─────────────────────────────────

class TestIsRelevantJob:
    def test_keeps_data_role(self):
        assert is_relevant_job("데이터 분석가") is True
        assert is_relevant_job("Senior Data Engineer") is True
        assert is_relevant_job("ML 엔지니어 (경력)") is True

    def test_drops_clear_mismatch(self):
        """관련 토큰 없고 무관 토큰만 있으면 제외."""
        assert is_relevant_job("영업관리 사원 모집") is False
        assert is_relevant_job("간호조무사 채용") is False
        assert is_relevant_job("주방 보조 구함") is False

    def test_relevant_token_overrides_irrelevant(self):
        """무관 토큰이 있어도 관련 토큰이 있으면 유지."""
        assert is_relevant_job("빅데이터 기반 영업 분석") is True

    def test_neutral_title_kept(self):
        """관련·무관 토큰 모두 없으면 보수적으로 유지."""
        assert is_relevant_job("플랫폼 개발자") is True

    def test_empty_dropped(self):
        assert is_relevant_job("") is False


# ── wanted 파서 ───────────────────────────────────────────────────

class TestWantedParsers:
    def test_salary_range(self):
        assert _parse_salary("3,500 ~ 5,000만원") == (3500, 5000)

    def test_salary_single(self):
        mn, mx = _parse_salary("4,000만원")
        assert mn == 4000
        assert mx is None

    def test_salary_none(self):
        assert _parse_salary(None) == (None, None)

    def test_salary_empty(self):
        assert _parse_salary("") == (None, None)

    def test_experience_range(self):
        assert _parse_experience("3년 ~ 7년") == (3, 7)

    def test_experience_newcomer(self):
        assert _parse_experience("신입") == (0, 0)

    def test_experience_irrelevant(self):
        assert _parse_experience("경력무관") == (None, None)

    def test_experience_none(self):
        assert _parse_experience(None) == (None, None)


# ── saramin 파서 ──────────────────────────────────────────────────

class TestSaraminParsers:
    def test_salary_range(self):
        assert _parse_salary_saramin("3500~5000만원") == (3500, 5000)

    def test_salary_negotiable(self):
        assert _parse_salary_saramin("면접 후 결정") == (None, None)

    def test_experience_newcomer(self):
        mn, mx = SaraminCrawler._parse_experience("신입")
        assert mn == 0 and mx == 0

    def test_experience_newcomer_and_career(self):
        """'신입·경력' 혼합은 경력무관으로 처리."""
        mn, mx = SaraminCrawler._parse_experience("신입·경력")
        assert mn is None and mx is None

    def test_experience_irrelevant(self):
        mn, mx = SaraminCrawler._parse_experience("경력무관")
        assert mn is None and mx is None

    def test_employment_normalization(self):
        assert SaraminCrawler._normalize_employment("정규직") == "정규직"
        assert SaraminCrawler._normalize_employment("계약직(기간제)") == "계약직"
        assert SaraminCrawler._normalize_employment("인턴십") == "인턴"
        assert SaraminCrawler._normalize_employment(None) is None


# ── jobkorea 파서 ─────────────────────────────────────────────────

class TestJobKoreaParsers:
    def test_experience_up_arrow(self):
        mn, mx = _parse_experience_jk("경력7년↑")
        assert mn == 7 and mx is None

    def test_experience_range(self):
        mn, mx = _parse_experience_jk("경력3~5년")
        assert mn == 3 and mx == 5

    def test_experience_newcomer(self):
        mn, mx = _parse_experience_jk("신입")
        assert mn == 0 and mx == 0

    def test_experience_irrelevant(self):
        mn, mx = _parse_experience_jk("경력무관")
        assert mn is None and mx is None

    def test_experience_newcomer_and_career(self):
        mn, mx = _parse_experience_jk("신입 및 경력")
        assert mn is None and mx is None

    def test_deadline_date(self):
        from datetime import date, datetime
        result = _parse_deadline_jk("05/01(금) 마감")
        assert result == date(datetime.now().year, 5, 1)

    def test_deadline_always_open(self):
        assert _parse_deadline_jk("상시채용") is None
        assert _parse_deadline_jk(None) is None

    def test_deadline_year_rollover_at_year_end(self, monkeypatch):
        """연말에 본 다음 해 초 마감일은 내년으로 보정."""
        import crawler.jobkorea as jk
        from datetime import date as real_date

        class FakeDate(real_date):
            @classmethod
            def today(cls):
                return real_date(2026, 12, 20)

        monkeypatch.setattr(jk, "date", FakeDate)
        assert jk._parse_deadline_jk("01/15(목) 마감") == real_date(2027, 1, 15)

    def test_deadline_no_rollover_for_near_future(self, monkeypatch):
        """가까운 미래 마감일은 현재 연도 유지."""
        import crawler.jobkorea as jk
        from datetime import date as real_date

        class FakeDate(real_date):
            @classmethod
            def today(cls):
                return real_date(2026, 6, 1)

        monkeypatch.setattr(jk, "date", FakeDate)
        assert jk._parse_deadline_jk("06/30(화) 마감") == real_date(2026, 6, 30)

    def test_employment_normalization(self):
        assert _normalize_employment_jk("정규직") == "정규직"
        assert _normalize_employment_jk("계약직") == "계약직"
        assert _normalize_employment_jk("인턴") == "인턴"
        assert _normalize_employment_jk(None) is None
        assert _normalize_employment_jk("미지원형") is None
