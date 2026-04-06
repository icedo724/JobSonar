"""시계열 트렌드 분석: 직군별·스킬별 공고 수 추이."""
import pandas as pd
import sqlite3
from pathlib import Path


def load_jobs_df(conn: sqlite3.Connection) -> pd.DataFrame:
    """jobs 테이블 전체를 DataFrame으로 로드."""
    return pd.read_sql_query(
        """
        SELECT j.id, j.title, j.company_name, j.job_category,
               j.industry, j.employment_type, j.source_site, j.url, j.location,
               j.experience_min, j.experience_max,
               j.salary_min, j.salary_max, j.posted_date,
               j.deadline_date, j.collected_at, j.is_active
        FROM jobs j
        WHERE j.is_active = 1 AND j.is_duplicate = 0
        """,
        conn,
        parse_dates=["posted_date", "deadline_date", "collected_at"],
    )


def load_skills_df(conn: sqlite3.Connection) -> pd.DataFrame:
    """job_skills + jobs 조인 DataFrame."""
    return pd.read_sql_query(
        """
        SELECT js.skill_name, j.job_category, j.source_site,
               j.posted_date, j.collected_at
        FROM job_skills js
        JOIN jobs j ON js.job_id = j.id
        WHERE j.is_active = 1 AND j.is_duplicate = 0
        """,
        conn,
        parse_dates=["posted_date", "collected_at"],
    )


def weekly_job_counts(jobs_df: pd.DataFrame) -> pd.DataFrame:
    """주별 직군별 공고 수 집계."""
    df = jobs_df.copy()
    df["week"] = df["collected_at"].dt.to_period("W").dt.start_time
    return (
        df.groupby(["week", "job_category"])
        .size()
        .reset_index(name="count")
        .sort_values("week")
    )


def top_skills_by_category(skills_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """직군별 상위 N개 스킬 (count, pct 포함)."""
    counts = (
        skills_df.groupby(["job_category", "skill_name"])
        .size()
        .reset_index(name="count")
    )
    totals = skills_df.groupby("job_category").size().reset_index(name="total")
    merged = counts.merge(totals, on="job_category")
    merged["pct"] = (merged["count"] / merged["total"] * 100).round(1)
    return (
        merged.sort_values(["job_category", "count"], ascending=[True, False])
        .groupby("job_category")
        .head(top_n)
        .drop(columns="total")
        .reset_index(drop=True)
    )


def skill_trend_weekly(skills_df: pd.DataFrame, skill_names: list[str]) -> pd.DataFrame:
    """지정 스킬들의 주별 언급 수 추이."""
    df = skills_df[skills_df["skill_name"].isin(skill_names)].copy()
    df["week"] = df["collected_at"].dt.to_period("W").dt.start_time
    return (
        df.groupby(["week", "skill_name"])
        .size()
        .reset_index(name="count")
        .sort_values(["week", "skill_name"])
    )


def salary_by_category(jobs_df: pd.DataFrame) -> pd.DataFrame:
    """직군별 연봉 데이터. salary_mid = (min+max)/2."""
    df = jobs_df.dropna(subset=["salary_min"]).copy()
    df["salary_mid"] = df[["salary_min", "salary_max"]].mean(axis=1)
    return df[["job_category", "salary_min", "salary_max", "salary_mid", "company_name"]]


def company_rankings(jobs_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """공고 수 기준 상위 기업."""
    counts = jobs_df.groupby("company_name").size().reset_index(name="count")
    cats = (
        jobs_df.groupby("company_name")["job_category"]
        .apply(lambda x: ", ".join(sorted(x.dropna().unique())))
        .reset_index(name="categories")
    )
    return (
        counts.merge(cats, on="company_name")
        .sort_values("count", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )


def location_distribution(jobs_df: pd.DataFrame) -> pd.DataFrame:
    """지역별 공고 수. 첫 번째 공백 앞(시 단위)으로 정제."""
    df = jobs_df.dropna(subset=["location"]).copy()
    df["city"] = df["location"].str.split().str[0]
    return (
        df.groupby(["city", "job_category"])
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )


def experience_distribution(jobs_df: pd.DataFrame) -> pd.DataFrame:
    """경력별 공고 수 분포."""
    df = jobs_df.dropna(subset=["experience_min"]).copy()
    bins = [-1, 0, 2, 4, 6, 9, 100]
    labels = ["신입", "1-2년", "3-4년", "5-6년", "7-9년", "10년+"]
    df["exp_group"] = pd.cut(df["experience_min"], bins=bins, labels=labels)
    return (
        df.groupby(["exp_group", "job_category"], observed=True)
        .size()
        .reset_index(name="count")
    )


def skill_growth_rate(skills_df: pd.DataFrame, top_n: int = 15) -> pd.DataFrame:
    """최근 2주 vs 이전 2주 스킬 언급 증감률."""
    df = skills_df.copy()
    if df.empty or "collected_at" not in df.columns:
        return pd.DataFrame(columns=["skill_name", "recent", "prev", "growth_pct"])

    latest = df["collected_at"].max()
    cut1 = latest - pd.Timedelta(weeks=2)
    cut2 = latest - pd.Timedelta(weeks=4)

    recent = (
        df[df["collected_at"] >= cut1]
        .groupby("skill_name").size().reset_index(name="recent")
    )
    prev = (
        df[(df["collected_at"] >= cut2) & (df["collected_at"] < cut1)]
        .groupby("skill_name").size().reset_index(name="prev")
    )
    merged = recent.merge(prev, on="skill_name", how="outer").fillna(0)
    merged["growth_pct"] = (
        ((merged["recent"] - merged["prev"]) / (merged["prev"] + 1) * 100)
        .round(1)
    )
    return (
        merged[merged["recent"] >= 3]
        .sort_values("growth_pct", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )


def new_jobs_count(jobs_df: pd.DataFrame, days: int = 7) -> int:
    """최근 N일 신규 공고 수."""
    if jobs_df.empty:
        return 0
    cutoff = jobs_df["collected_at"].max() - pd.Timedelta(days=days)
    return int((jobs_df["collected_at"] >= cutoff).sum())


def load_jobs_for_board(conn: sqlite3.Connection) -> pd.DataFrame:
    """공고 목록 탭용: 스킬 태그 포함하여 로드."""
    jobs = pd.read_sql_query(
        """
        SELECT j.id, j.title, j.company_name, j.job_category,
               j.industry, j.employment_type, j.source_site, j.url, j.location,
               j.experience_min, j.experience_max,
               j.salary_min, j.salary_max,
               j.posted_date, j.deadline_date, j.collected_at
        FROM jobs j
        WHERE j.is_active = 1 AND j.is_duplicate = 0
        ORDER BY j.collected_at DESC
        """,
        conn,
        parse_dates=["posted_date", "deadline_date", "collected_at"],
    )
    skills = pd.read_sql_query(
        "SELECT job_id, skill_name FROM job_skills",
        conn,
    )
    skill_agg = (
        skills.groupby("job_id")["skill_name"]
        .apply(lambda x: " · ".join(sorted(x)))
        .reset_index(name="skills")
    )
    return jobs.merge(skill_agg, left_on="id", right_on="job_id", how="left").drop(columns="job_id")
