"""시계열 트렌드 분석: 직군별·스킬별 공고 수 추이."""
import pandas as pd
import sqlite3

_REGIONS = ["서울", "경기", "인천", "부산", "대구", "대전", "광주", "울산", "세종",
            "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주"]


def normalize_location(loc) -> str:
    """지역명을 시·도 단위로 정규화. trends.py와 dashboard 양쪽에서 공유."""
    if not loc or (isinstance(loc, float) and pd.isna(loc)):
        return ""
    s = str(loc)
    for r in _REGIONS:
        if s.startswith(r) or r in s:
            return r
    return "해외"


def load_jobs_df(conn: sqlite3.Connection) -> pd.DataFrame:
    """활성·비중복 공고를 DataFrame으로 로드."""
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
    """job_skills + jobs 조인 DataFrame. job_id 포함 (pct 계산 기준)."""
    return pd.read_sql_query(
        """
        SELECT js.job_id, js.skill_name, j.job_category, j.source_site,
               j.posted_date, j.collected_at
        FROM job_skills js
        JOIN jobs j ON js.job_id = j.id
        WHERE j.is_active = 1 AND j.is_duplicate = 0
        """,
        conn,
        parse_dates=["posted_date", "collected_at"],
    )


def _trend_date(df: pd.DataFrame) -> pd.Series:
    """posted_date 우선 사용, NULL이면 collected_at으로 폴백.
    posted_date를 명시적으로 datetime으로 변환해 object dtype 혼재를 방지.
    """
    posted = pd.to_datetime(df["posted_date"], errors="coerce")
    return posted.fillna(df["collected_at"])


def weekly_job_counts(jobs_df: pd.DataFrame) -> pd.DataFrame:
    """주별 직군별 공고 수 집계. posted_date 기준 (없으면 collected_at 폴백)."""
    if jobs_df.empty:
        return pd.DataFrame(columns=["week", "job_category", "count"])
    df = jobs_df.copy()
    df["week"] = _trend_date(df).dt.to_period("W").dt.start_time.dt.strftime("%Y-%m-%d")
    return (
        df.groupby(["week", "job_category"])
        .size()
        .reset_index(name="count")
        .sort_values("week")
    )


def top_skills_by_category(
    skills_df: pd.DataFrame,
    jobs_df: pd.DataFrame | None = None,
    top_n: int = 20,
) -> pd.DataFrame:
    """직군별 상위 N개 스킬.
    count: 해당 스킬이 등장한 공고 수
    pct: 직군 내 전체 공고 수 대비 비율 (%)
    """
    counts = (
        skills_df.groupby(["job_category", "skill_name"])
        .size()
        .reset_index(name="count")
    )
    # 분모: jobs_df 전달 시 고유 공고 수 기준, 없으면 skills_df의 job_id 기준
    if jobs_df is not None and not jobs_df.empty:
        totals = jobs_df.groupby("job_category").size().reset_index(name="total")
    else:
        totals = (
            skills_df.groupby("job_category")["job_id"]
            .nunique()
            .reset_index(name="total")
        )
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
    """지정 스킬들의 주별 언급 수 추이. posted_date 기준 (없으면 collected_at 폴백)."""
    df = skills_df[skills_df["skill_name"].isin(skill_names)].copy()
    df["week"] = _trend_date(df).dt.to_period("W").dt.start_time.dt.strftime("%Y-%m-%d")
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
    """지역별(시·도 단위) 공고 수."""
    df = jobs_df.dropna(subset=["location"]).copy()
    df["city"] = df["location"].apply(normalize_location)
    df = df[df["city"] != ""]
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
    """최근 2주 vs 이전 2주 스킬 언급 증감률.
    recent >= 3 AND prev >= 2 조건으로 저빈도 노이즈 제거.
    """
    df = skills_df.copy()
    if df.empty:
        return pd.DataFrame(columns=["skill_name", "recent", "prev", "growth_pct"])

    date_col = _trend_date(df)
    latest = date_col.max()
    cut1 = latest - pd.Timedelta(weeks=2)
    cut2 = latest - pd.Timedelta(weeks=4)

    recent = (
        df[date_col >= cut1]
        .groupby("skill_name").size().reset_index(name="recent")
    )
    prev = (
        df[(date_col >= cut2) & (date_col < cut1)]
        .groupby("skill_name").size().reset_index(name="prev")
    )
    merged = recent.merge(prev, on="skill_name", how="outer").fillna(0)
    merged["growth_pct"] = (
        ((merged["recent"] - merged["prev"]) / (merged["prev"] + 1) * 100)
        .round(1)
    )
    return (
        merged[(merged["recent"] >= 3) & (merged["prev"] >= 2)]
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
    """공고 목록 탭용: 스킬 태그 포함 로드. SQL에서 활성·비중복 필터링."""
    jobs = pd.read_sql_query(
        """
        SELECT j.id, j.title, j.company_name, j.job_category,
               j.industry, j.employment_type, j.source_site, j.url, j.location,
               j.experience_min, j.experience_max,
               j.salary_min, j.salary_max, j.description,
               j.posted_date, j.deadline_date, j.collected_at
        FROM jobs j
        WHERE j.is_active = 1 AND j.is_duplicate = 0
        ORDER BY j.collected_at DESC
        """,
        conn,
        parse_dates=["posted_date", "deadline_date", "collected_at"],
    )
    skills = pd.read_sql_query(
        """
        SELECT js.job_id, js.skill_name
        FROM job_skills js
        JOIN jobs j ON js.job_id = j.id
        WHERE j.is_active = 1 AND j.is_duplicate = 0
        """,
        conn,
    )
    skill_agg = (
        skills.groupby("job_id")["skill_name"]
        .apply(lambda x: " · ".join(sorted(x)))
        .reset_index(name="skills")
    )
    jobs = jobs.merge(skill_agg, left_on="id", right_on="job_id", how="left").drop(columns="job_id")

    # 중복 그룹화: 같은 공고가 올라온 다른 플랫폼들을 대표 공고에 묶어 표시
    dups = pd.read_sql_query(
        """
        SELECT duplicate_of AS cid, GROUP_CONCAT(DISTINCT source_site) AS extra_sources
        FROM jobs
        WHERE duplicate_of IS NOT NULL AND is_active = 1
        GROUP BY duplicate_of
        """,
        conn,
    )
    jobs = jobs.merge(dups, left_on="id", right_on="cid", how="left").drop(
        columns="cid", errors="ignore"
    )
    return jobs
