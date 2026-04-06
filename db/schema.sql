-- JobSonar Database Schema
-- SQLite (로컬 개발) / PostgreSQL (프로덕션) 호환

-- 채용공고 메인 테이블
CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_site     TEXT    NOT NULL,           -- 'wanted' | 'saramin' | 'jobkorea'
    source_id       TEXT    NOT NULL,           -- 사이트별 고유 ID (중복 방지)
    url             TEXT    NOT NULL,
    title           TEXT    NOT NULL,           -- 공고 제목
    company_name    TEXT    NOT NULL,
    job_category    TEXT,                       -- '데이터 엔지니어' | '데이터 분석가' 등
    industry        TEXT,                       -- 회사 업종 (원티드: API, 사람인/잡코리아: HTML)
    employment_type TEXT,                       -- '정규직' | '계약직' | '인턴' 등
    location        TEXT,                       -- '서울' | '판교' 등
    experience_min  INTEGER,                    -- 최소 경력 (년)
    experience_max  INTEGER,                    -- 최대 경력 (년), NULL = 무관
    salary_min      INTEGER,                    -- 연봉 하한 (만원)
    salary_max      INTEGER,                    -- 연봉 상한 (만원)
    posted_date     DATE,                       -- 공고 게시일
    deadline_date   DATE,                       -- 마감일
    is_active       BOOLEAN NOT NULL DEFAULT 1,
    is_duplicate    BOOLEAN NOT NULL DEFAULT 0, -- 타 사이트 동일 공고 중복 여부
    collected_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(source_site, source_id)
);

-- 기술 스택 테이블 (공고별 태그)
CREATE TABLE IF NOT EXISTS job_skills (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    skill_name  TEXT    NOT NULL,               -- 정규화된 스킬명 (소문자)
    UNIQUE(job_id, skill_name)
);

-- 수집 실행 로그 (GitHub Actions 디버깅용)
CREATE TABLE IF NOT EXISTS crawl_logs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_site     TEXT    NOT NULL,
    started_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at     DATETIME,
    jobs_found      INTEGER DEFAULT 0,
    jobs_inserted   INTEGER DEFAULT 0,
    jobs_updated    INTEGER DEFAULT 0,
    status          TEXT    NOT NULL DEFAULT 'running', -- 'running' | 'success' | 'failed'
    error_message   TEXT
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_jobs_source      ON jobs(source_site, source_id);
CREATE INDEX IF NOT EXISTS idx_jobs_dedup       ON jobs(lower(company_name), lower(title));
CREATE INDEX IF NOT EXISTS idx_jobs_category    ON jobs(job_category);
CREATE INDEX IF NOT EXISTS idx_jobs_posted      ON jobs(posted_date);
CREATE INDEX IF NOT EXISTS idx_jobs_active      ON jobs(is_active);
CREATE INDEX IF NOT EXISTS idx_job_skills_name  ON job_skills(skill_name);
CREATE INDEX IF NOT EXISTS idx_job_skills_job   ON job_skills(job_id);
