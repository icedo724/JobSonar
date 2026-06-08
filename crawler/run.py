"""
크롤러 실행 진입점.
GitHub Actions 또는 로컬에서 직접 실행:
  python -m crawler.run
  python -m crawler.run --source wanted --max-pages 5
"""
import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests as _requests

sys.path.insert(0, str(Path(__file__).parent.parent))

from crawler import WantedCrawler, SaraminCrawler, JobKoreaCrawler
from crawler.base import is_relevant_job
from db.connection import (
    init_db, get_conn, upsert_job, insert_skills,
    deactivate_unseen_jobs, deactivate_expired_jobs,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


_CRAWL_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _job_exists(conn, source_site: str, source_id: str) -> bool:
    """해당 공고가 이미 DB에 있는지 여부 (신규 공고만 상세 보강하기 위함)."""
    row = conn.execute(
        "SELECT 1 FROM jobs WHERE source_site=? AND source_id=?",
        (source_site, source_id),
    ).fetchone()
    return row is not None


def validate_job_links(
    conn,
    session=None,
    max_checks: int = 30,
    delay: float = 1.5,
) -> dict[str, int]:
    """활성 공고 URL 유효성 검사 (HTTP HEAD 요청).

    대상: 마감일 없는 활성 공고 중 최근 14일 내 수집된 것 (최대 max_checks건).
    HTTP 4xx 응답 → is_active=0 (링크 만료 확인).
    네트워크 오류는 false negative 허용(무시) — 보수적 접근.

    Args:
        conn: SQLite 커넥션
        session: requests.Session (테스트 시 mock 주입용). None이면 신규 생성.
        max_checks: 한 번에 검사할 최대 URL 수 (요청 수 제한)
        delay: 요청 사이 대기 시간(초). 테스트 시 0 전달.
    """
    own_session = session is None
    if own_session:
        session = _requests.Session()
        session.headers["User-Agent"] = _CRAWL_UA

    jobs = conn.execute(
        """
        SELECT id, url, source_site FROM jobs
        WHERE is_active = 1
          AND deadline_date IS NULL
          AND collected_at >= datetime('now', '-14 days')
        ORDER BY collected_at DESC
        LIMIT ?
        """,
        (max_checks,),
    ).fetchall()

    checked = 0
    deactivated = 0

    for job in jobs:
        try:
            resp = session.head(job["url"], allow_redirects=True, timeout=10)
            checked += 1
            if resp.status_code >= 400:
                conn.execute(
                    "UPDATE jobs SET is_active=0, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (job["id"],),
                )
                deactivated += 1
                logger.info(
                    f"[{job['source_site']}] HTTP {resp.status_code} → 링크 만료 비활성화"
                )
        except Exception as e:
            logger.debug(f"[URL 검사] 오류 (무시): {e}")

        if delay > 0:
            time.sleep(delay)

    if own_session:
        session.close()

    return {"checked": checked, "deactivated": deactivated}


def run_crawler(source: str, max_pages: int) -> dict:
    """크롤러 실행 후 통계 반환."""
    crawlers = {
        "wanted": WantedCrawler,
        "saramin": SaraminCrawler,
        "jobkorea": JobKoreaCrawler,
    }
    CrawlerClass = crawlers[source]
    crawler = CrawlerClass()

    # 크롤 시작 시각 기록 (UTC) — 이 시각보다 updated_at이 이전인 공고 = 오늘 미발견
    crawl_start = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    raw_jobs = crawler.crawl_all_categories(max_pages=max_pages)

    # 관련성 필터: 키워드 검색이 끌어온 무관 공고(영업·세무 등) 제외
    jobs = [j for j in raw_jobs if is_relevant_job(j.title)]
    skipped = len(raw_jobs) - len(jobs)
    if skipped:
        logger.info(f"[{source}] 관련성 필터로 {skipped}건 제외")

    stats = {"found": len(jobs), "inserted": 0, "updated": 0,
             "deactivated": 0, "errors": 0, "skipped_irrelevant": skipped}

    with get_conn() as conn:
        log_id = conn.execute(
            "INSERT INTO crawl_logs (source_site, status) VALUES (?, 'running')",
            (source,),
        ).lastrowid

        try:
            for job in jobs:
                try:
                    # 신규 공고만 상세 페이지로 보강 (요청 수 제한) — best-effort
                    if not _job_exists(conn, job.source_site, job.source_id):
                        try:
                            crawler.enrich(job)
                        except Exception as e:
                            logger.warning(f"[{source}] 상세 보강 실패 (무시): {e}")
                    job_id, action = upsert_job(conn, job.to_db_dict())
                    insert_skills(conn, job_id, job.skills)
                    stats[action] = stats.get(action, 0) + 1
                except Exception as e:
                    logger.error(f"DB 저장 실패: {e} — {job.source_id}")
                    stats["errors"] += 1

            # 오늘 크롤에서 발견 안 된 공고 즉시 비활성화
            stats["deactivated"] = deactivate_unseen_jobs(conn, source, crawl_start)

            conn.execute(
                """
                UPDATE crawl_logs
                SET finished_at=CURRENT_TIMESTAMP, status='success',
                    jobs_found=:found, jobs_inserted=:inserted, jobs_updated=:updated
                WHERE id=:log_id
                """,
                {**stats, "log_id": log_id},
            )
        except Exception as e:
            conn.execute(
                """
                UPDATE crawl_logs
                SET finished_at=CURRENT_TIMESTAMP, status='failed', error_message=?
                WHERE id=?
                """,
                (str(e), log_id),
            )
            raise

    return stats


def main():
    parser = argparse.ArgumentParser(description="JobSonar 크롤러")
    parser.add_argument(
        "--source", choices=["wanted", "saramin", "jobkorea", "all"], default="all",
        help="수집 대상 사이트"
    )
    parser.add_argument(
        "--max-pages", type=int, default=10,
        help="사이트당 최대 페이지 수 (기본: 10, 약 200건)"
    )
    args = parser.parse_args()

    sources = ["wanted", "saramin", "jobkorea"] if args.source == "all" else [args.source]

    init_db()

    for source in sources:
        logger.info(f"=== {source.upper()} 크롤링 시작 ===")
        stats = run_crawler(source, args.max_pages)
        logger.info(
            f"=== {source.upper()} 완료 — "
            f"발견 {stats['found']}건 / "
            f"신규 {stats.get('inserted', 0)}건 / "
            f"업데이트 {stats.get('updated', 0)}건 / "
            f"무관 제외 {stats.get('skipped_irrelevant', 0)}건 / "
            f"당일 미발견 비활성화 {stats.get('deactivated', 0)}건 / "
            f"오류 {stats['errors']}건 ==="
        )

    with get_conn() as conn:
        # 마감일 초과 공고 비활성화 (deadline_date 명시된 공고 한정)
        n_deadline = deactivate_expired_jobs(conn)
        if n_deadline:
            logger.info(f"마감일 초과 비활성화: {n_deadline}건")

        # 활성 공고 URL 유효성 검사 (HTTP HEAD) — 만료 링크 비활성화
        link_stats = validate_job_links(conn, max_checks=60)
        logger.info(
            f"링크 검증 — 검사 {link_stats['checked']}건 / "
            f"만료 비활성화 {link_stats['deactivated']}건"
        )


if __name__ == "__main__":
    main()
