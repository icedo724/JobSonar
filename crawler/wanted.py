"""원티드 크롤러 — SPA이므로 공개 REST API(/api/v4/jobs) 사용."""
import logging
import re
from datetime import date, datetime

from .base import BaseCrawler, JobItem, normalize_skill, extract_skills_from_text

logger = logging.getLogger(__name__)

# 원티드 직군 태그 ID
WANTED_TAG_IDS: dict[str, int] = {
    "데이터 엔지니어": 872,
    "데이터 분석가": 876,
    "데이터 사이언티스트": 873,
    "ML 엔지니어": 877,
}

WANTED_API = "https://www.wanted.co.kr/api/v4/jobs"
WANTED_JOB_API = "https://www.wanted.co.kr/api/v4/jobs/{job_id}"
WANTED_JOB_URL = "https://www.wanted.co.kr/wd/{job_id}"


def _parse_salary(salary_str: str | None) -> tuple[int | None, int | None]:
    """'3,500 ~ 5,000만원' 형식 파싱 → (3500, 5000)."""
    if not salary_str:
        return None, None
    nums = re.findall(r"[\d,]+", salary_str.replace(" ", ""))
    nums = [int(n.replace(",", "")) for n in nums]
    if len(nums) >= 2:
        return nums[0], nums[1]
    if len(nums) == 1:
        return nums[0], None
    return None, None


def _parse_experience(exp_str: str | None) -> tuple[int | None, int | None]:
    """'3년 ~ 7년' 또는 '신입' 형식 파싱."""
    if not exp_str:
        return None, None
    if "신입" in exp_str:
        return 0, 0
    if "경력무관" in exp_str:
        return None, None
    nums = re.findall(r"\d+", exp_str)
    nums = [int(n) for n in nums]
    if len(nums) >= 2:
        return nums[0], nums[1]
    if len(nums) == 1:
        return nums[0], None
    return None, None


class WantedCrawler(BaseCrawler):
    SITE_NAME = "wanted"
    MIN_DELAY = 1.5
    MAX_DELAY = 3.0

    def __init__(self):
        super().__init__()
        self.session.headers.update({
            "Referer": "https://www.wanted.co.kr/",
            "X-Want-App": "ios_1",
            "wantedsessionid": "",
        })

    def crawl(self, category: str, max_pages: int = 10) -> list[JobItem]:
        tag_id = WANTED_TAG_IDS.get(category)
        if tag_id is None:
            raise ValueError(f"Unknown category: {category}. "
                             f"Available: {list(WANTED_TAG_IDS.keys())}")

        jobs: list[JobItem] = []
        offset = 0
        limit = 20

        for page in range(max_pages):
            logger.info(f"[wanted] {category} — page {page + 1} (offset={offset})")

            try:
                resp = self._get(
                    WANTED_API,
                    params={
                        "country": "kr",
                        "tag_type_ids": tag_id,
                        "job_sort": "job.latest_order",
                        "years": -1,        # 경력무관 포함
                        "locations": "all",
                        "offset": offset,
                        "limit": limit,
                    },
                )
                data = resp.json()
            except Exception as e:
                logger.error(f"[wanted] 목록 API 오류 (offset={offset}): {e}")
                break

            items = data.get("data", [])
            if not items:
                logger.info(f"[wanted] 더 이상 공고 없음 (offset={offset})")
                break

            for raw in items:
                job = self._parse_list_item(raw, category)
                if job:
                    jobs.append(job)

            # 원티드 API: links.next가 없으면 마지막 페이지
            if not data.get("links", {}).get("next"):
                break

            offset += limit

        logger.info(f"[wanted] {category} 총 {len(jobs)}건 수집 완료")
        return jobs

    def crawl_all_categories(self, max_pages: int = 10) -> list[JobItem]:
        """전체 대상 직군 수집."""
        all_jobs: list[JobItem] = []
        for category in WANTED_TAG_IDS:
            all_jobs.extend(self.crawl(category, max_pages=max_pages))
        return all_jobs

    def fetch_detail(self, job_id: str) -> dict | None:
        """개별 공고 상세 API 호출 (스킬 태그 보강용)."""
        try:
            resp = self._get(WANTED_JOB_API.format(job_id=job_id))
            return resp.json().get("job", {})
        except Exception as e:
            logger.warning(f"[wanted] 상세 조회 실패 job_id={job_id}: {e}")
            return None

    def _parse_list_item(self, raw: dict, category: str) -> JobItem | None:
        try:
            job_id = str(raw["id"])
            title = raw.get("position", "").strip()
            company = raw.get("company", {}).get("name", "").strip()
            location = raw.get("address", {}).get("location", "")

            skill_tags: list[str] = [
                normalize_skill(t["keyword"])
                for t in raw.get("tags", [])
                if t.get("kind") == "skill"
            ]
            if not skill_tags:
                skill_tags = extract_skills_from_text(title)

            sal_min, sal_max = _parse_salary(raw.get("salary_type", {}).get("name"))
            exp_min, exp_max = _parse_experience(raw.get("experience_level", {}).get("name"))

            posted_date = None
            if raw.get("published_at"):
                try:
                    posted_date = datetime.fromisoformat(
                        raw["published_at"].replace("Z", "+00:00")
                    ).date()
                except ValueError:
                    pass

            industry = raw.get("company", {}).get("industry_name") or None

            return JobItem(
                source_site="wanted",
                source_id=job_id,
                url=WANTED_JOB_URL.format(job_id=job_id),
                title=title,
                company_name=company,
                job_category=category,
                industry=industry,
                skills=skill_tags,
                location=location,
                experience_min=exp_min,
                experience_max=exp_max,
                salary_min=sal_min,
                salary_max=sal_max,
                posted_date=posted_date,
            )
        except Exception as e:
            logger.warning(f"[wanted] 파싱 실패 (raw={raw.get('id')}): {e}")
            return None
