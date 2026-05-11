"""분석 함수 단위 테스트."""
import sys
from pathlib import Path
from datetime import date, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import pytest
from analysis.trends import (
    normalize_location,
    weekly_job_counts,
    top_skills_by_category,
    skill_trend_weekly,
    salary_by_category,
    experience_distribution,
    skill_growth_rate,
    new_jobs_count,
    _trend_date,
)


# ── normalize_location ────────────────────────────────────────────

class TestNormalizeLocation:
    def test_seoul(self):
        assert normalize_location("서울 강남구") == "서울"

    def test_gyeonggi(self):
        assert normalize_location("경기 성남시 분당구") == "경기"

    def test_exact_match(self):
        assert normalize_location("부산") == "부산"

    def test_overseas(self):
        assert normalize_location("San Francisco") == "해외"

    def test_none(self):
        assert normalize_location(None) == ""

    def test_nan(self):
        assert normalize_location(float("nan")) == ""


# ── _trend_date ───────────────────────────────────────────────────

class TestTrendDate:
    def _make_df(self, posted, collected):
        return pd.DataFrame({
            "posted_date": pd.to_datetime(posted),
            "collected_at": pd.to_datetime(collected),
        })

    def test_prefers_posted_date(self):
        df = self._make_df(["2025-01-10"], ["2025-01-15"])
        result = _trend_date(df)
        assert str(result.iloc[0].date()) == "2025-01-10"

    def test_fallback_to_collected_at(self):
        df = self._make_df([None], ["2025-01-15"])
        result = _trend_date(df)
        assert str(result.iloc[0].date()) == "2025-01-15"


# ── weekly_job_counts ─────────────────────────────────────────────

class TestWeeklyJobCounts:
    def _make_jobs(self):
        today = pd.Timestamp.now()
        return pd.DataFrame({
            "job_category": ["데이터 엔지니어", "데이터 분석가", "데이터 엔지니어"],
            "posted_date":  [today - timedelta(days=3), today - timedelta(days=10), None],
            "collected_at": [today, today, today],
        })

    def test_returns_dataframe(self):
        df = self._make_jobs()
        result = weekly_job_counts(df)
        assert isinstance(result, pd.DataFrame)
        assert "week" in result.columns
        assert "count" in result.columns

    def test_count_correct(self):
        df = self._make_jobs()
        result = weekly_job_counts(df)
        assert result["count"].sum() == 3

    def test_empty_input(self):
        empty = pd.DataFrame(columns=["job_category", "posted_date", "collected_at"])
        result = weekly_job_counts(empty)
        assert result.empty


# ── top_skills_by_category ────────────────────────────────────────

class TestTopSkillsByCategory:
    def _make_skills(self):
        return pd.DataFrame({
            "job_id":       [1, 1, 2, 2, 3],
            "skill_name":   ["Python", "SQL", "Python", "AWS", "Python"],
            "job_category": ["DE", "DE", "DE", "DE", "DA"],
            "source_site":  ["wanted"] * 5,
            "posted_date":  [None] * 5,
            "collected_at": [pd.Timestamp.now()] * 5,
        })

    def _make_jobs(self):
        return pd.DataFrame({
            "id":           [1, 2, 3],
            "job_category": ["DE", "DE", "DA"],
        })

    def test_basic(self):
        sf = self._make_skills()
        result = top_skills_by_category(sf, top_n=10)
        assert "skill_name" in result.columns
        assert "count" in result.columns
        assert "pct" in result.columns

    def test_pct_with_jobs_df(self):
        sf = self._make_skills()
        jf = self._make_jobs()
        result = top_skills_by_category(sf, jobs_df=jf, top_n=10)
        de = result[result["job_category"] == "DE"]
        python_row = de[de["skill_name"] == "Python"].iloc[0]
        # DE 공고 2개 중 Python은 2개 → 100%
        assert python_row["pct"] == 100.0

    def test_top_n_limit(self):
        sf = self._make_skills()
        result = top_skills_by_category(sf, top_n=1)
        # 각 직군에서 1개만
        assert result.groupby("job_category").size().max() == 1

    def test_empty_input(self):
        empty = pd.DataFrame(columns=["job_id", "skill_name", "job_category",
                                       "source_site", "posted_date", "collected_at"])
        result = top_skills_by_category(empty)
        assert result.empty


# ── salary_by_category ────────────────────────────────────────────

class TestSalaryByCategory:
    def _make_jobs(self):
        return pd.DataFrame({
            "job_category": ["DE", "DA", "DE"],
            "salary_min":   [4000, 3000, None],
            "salary_max":   [6000, 5000, None],
            "company_name": ["A", "B", "C"],
        })

    def test_drops_null_salary(self):
        df = self._make_jobs()
        result = salary_by_category(df)
        assert len(result) == 2  # None 행 제거

    def test_salary_mid_calculation(self):
        df = self._make_jobs()
        result = salary_by_category(df)
        de_row = result[result["job_category"] == "DE"].iloc[0]
        assert de_row["salary_mid"] == 5000.0  # (4000+6000)/2


# ── experience_distribution ───────────────────────────────────────

class TestExperienceDistribution:
    def _make_jobs(self):
        return pd.DataFrame({
            "job_category":   ["DE"] * 5,
            "experience_min": [0, 1, 3, 7, None],
            "collected_at":   [pd.Timestamp.now()] * 5,
        })

    def test_basic_groups(self):
        df = self._make_jobs()
        result = experience_distribution(df)
        labels = result["exp_group"].astype(str).tolist()
        assert "신입" in labels
        assert "1-2년" in labels

    def test_null_excluded(self):
        df = self._make_jobs()
        result = experience_distribution(df)
        assert result["count"].sum() == 4  # None 제외


# ── skill_growth_rate ─────────────────────────────────────────────

class TestSkillGrowthRate:
    def _make_skills(self, recent_count=5, prev_count=2):
        now = pd.Timestamp.now()
        rows = []
        for _ in range(recent_count):
            rows.append({"skill_name": "Python", "job_id": 1,
                          "job_category": "DE", "source_site": "wanted",
                          "posted_date": None, "collected_at": now - timedelta(days=3)})
        for _ in range(prev_count):
            rows.append({"skill_name": "Python", "job_id": 2,
                          "job_category": "DE", "source_site": "wanted",
                          "posted_date": None, "collected_at": now - timedelta(days=20)})
        return pd.DataFrame(rows)

    def test_returns_dataframe(self):
        df = self._make_skills()
        result = skill_growth_rate(df)
        assert isinstance(result, pd.DataFrame)

    def test_growth_positive(self):
        df = self._make_skills(recent_count=5, prev_count=2)
        result = skill_growth_rate(df)
        assert not result.empty
        assert result.iloc[0]["growth_pct"] > 0

    def test_prev_filter_removes_noise(self):
        """prev < 2인 스킬은 포함하지 않아야 함."""
        df = self._make_skills(recent_count=5, prev_count=1)
        result = skill_growth_rate(df)
        assert result.empty  # prev=1 이므로 필터링

    def test_empty_input(self):
        empty = pd.DataFrame(columns=["skill_name", "job_id", "job_category",
                                       "source_site", "posted_date", "collected_at"])
        result = skill_growth_rate(empty)
        assert result.empty


# ── new_jobs_count ────────────────────────────────────────────────

class TestNewJobsCount:
    def test_recent_jobs(self):
        now = pd.Timestamp.now()
        df = pd.DataFrame({
            "collected_at": [now - timedelta(days=i) for i in range(10)],
        })
        assert new_jobs_count(df, days=7) == 8  # 0~7일 포함

    def test_empty(self):
        assert new_jobs_count(pd.DataFrame(columns=["collected_at"]), days=7) == 0
