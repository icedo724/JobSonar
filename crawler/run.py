"""
크롤러 실행 진입점.
GitHub Actions 또는 로컬에서 직접 실행:
  python -m crawler.run
  python -m crawler.run --source wanted --max-pages 5
"""
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from crawler import WantedCrawler, SaraminCrawler, JobKoreaCrawler
from db.connection import init_db, get_conn, upsert_job, insert_skills, deactivate_expired_jobs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_crawler(source: str, max_pages: int) -> dict:
    """크롤러 실행 후 통계 반환."""
    crawlers = {
        "wanted": WantedCrawler,
        "saramin": SaraminCrawler,
        "jobkorea": JobKoreaCrawler,
    }
    CrawlerClass = crawlers[source]
    crawler = CrawlerClass()
    jobs = crawler.crawl_all_categories(max_pages=max_pages)

    stats = {"found": len(jobs), "inserted": 0, "updated": 0, "errors": 0}

    with get_conn() as conn:
        log_id = conn.execute(
            "INSERT INTO crawl_logs (source_site, status) VALUES (?, 'running')",
            (source,),
        ).lastrowid

        try:
            for job in jobs:
                try:
                    job_id, action = upsert_job(conn, job.to_db_dict())
                    insert_skills(conn, job_id, job.skills)
                    stats[action] = stats.get(action, 0) + 1
                except Exception as e:
                    logger.error(f"DB 저장 실패: {e} — {job.source_id}")
                    stats["errors"] += 1

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
            f"오류 {stats['errors']}건 ==="
        )

    # 전체 크롤 완료 후 만료 공고 비활성화
    with get_conn() as conn:
        expired = deactivate_expired_jobs(conn)
    logger.info(
        f"만료 공고 비활성화 — "
        f"마감일 초과: {expired['by_deadline']}건, "
        f"7일 미발견(마감일 없음): {expired['by_staleness']}건"
    )


if __name__ == "__main__":
    main()
