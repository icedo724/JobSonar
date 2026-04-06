"""사람인 크롤러 — robots.txt Disallow 준수, 상세 페이지 미수집."""
import logging
import re
from datetime import date, datetime
from urllib.parse import urlencode

from bs4 import BeautifulSoup

from .base import BaseCrawler, JobItem, normalize_skill, extract_skills_from_text

logger = logging.getLogger(__name__)

# 사람인 직군 검색 키워드
SARAMIN_KEYWORDS: dict[str, str] = {
    "데이터 엔지니어": "데이터엔지니어",
    "데이터 분석가": "데이터분석가",
    "데이터 사이언티스트": "데이터사이언티스트",
    "ML 엔지니어": "머신러닝엔지니어",
}

SARAMIN_BASE = "https://www.saramin.co.kr"
SARAMIN_LIST = f"{SARAMIN_BASE}/zf_user/search/recruit"


def _parse_salary_saramin(text: str | None) -> tuple[int | None, int | None]:
    """'3500~5000만원' 또는 '면접 후 결정' 파싱."""
    if not text:
        return None, None
    nums = re.findall(r"\d{3,5}", text.replace(",", ""))
    nums = [int(n) for n in nums]
    if len(nums) >= 2:
        return nums[0], nums[1]
    if len(nums) == 1:
        return nums[0], None
    return None, None


class SaraminCrawler(BaseCrawler):
    SITE_NAME = "saramin"
    MIN_DELAY = 2.0
    MAX_DELAY = 4.0

    def crawl(self, category: str, max_pages: int = 10) -> list[JobItem]:
        keyword = SARAMIN_KEYWORDS.get(category)
        if keyword is None:
            raise ValueError(f"Unknown category: {category}")

        jobs: list[JobItem] = []

        for page in range(1, max_pages + 1):
            logger.info(f"[saramin] {category} — page {page}")

            params = {
                "searchType": "search",
                "searchword": keyword,
                "recruitPage": page,
                "recruitPageCount": 40,
                "order": "reg_dt",   # 최신순
                "recruitSort": "reg_dt",
                "jobtype": 1,        # 정규직 위주
            }

            try:
                url = f"{SARAMIN_LIST}?{urlencode(params)}"
                soup = self._soup(url)
            except Exception as e:
                logger.error(f"[saramin] 목록 페이지 오류 (page={page}): {e}")
                break

            items = soup.select(".item_recruit")
            if not items:
                logger.info(f"[saramin] 더 이상 공고 없음 (page={page})")
                break

            for item in items:
                job = self._parse_item(item, category)
                if job:
                    jobs.append(job)

        logger.info(f"[saramin] {category} 총 {len(jobs)}건 수집 완료")
        return jobs

    def crawl_all_categories(self, max_pages: int = 10) -> list[JobItem]:
        all_jobs: list[JobItem] = []
        for category in SARAMIN_KEYWORDS:
            all_jobs.extend(self.crawl(category, max_pages=max_pages))
        return all_jobs

    def _parse_item(self, item: BeautifulSoup, category: str) -> JobItem | None:
        try:
            # ID는 value 속성에 있음
            job_id = item.get("value", "")
            if not job_id:
                return None

            title_el = item.select_one(".job_tit a")
            title = title_el.get("title", "").strip() if title_el else ""

            company_el = item.select_one(".corp_name a")
            company = company_el.get_text(strip=True) if company_el else ""

            # span 순서: [지역, 경력, 학력, 고용형태]
            conditions = item.select(".job_condition span")
            location     = conditions[0].get_text(strip=True) if len(conditions) > 0 else None
            exp_str      = conditions[1].get_text(strip=True) if len(conditions) > 1 else None
            exp_min, exp_max = self._parse_experience(exp_str)
            emp_raw      = conditions[3].get_text(strip=True) if len(conditions) > 3 else None
            employment_type = self._normalize_employment(emp_raw)

            salary_el = item.select_one(".salary")
            sal_min, sal_max = _parse_salary_saramin(
                salary_el.get_text(strip=True) if salary_el else None
            )

            deadline_el = item.select_one(".job_date .date")
            deadline_date = self._parse_deadline(
                deadline_el.get_text(strip=True) if deadline_el else None
            )

            # 사람인: sector 태그 + 제목 + 직무태그 전부 합쳐서 추출
            skill_parts = [title]
            skill_area = item.select_one(".job_sector")
            if skill_area:
                skill_parts.append(skill_area.get_text(" ", strip=True))
            # 추가 태그 영역
            for tag_el in item.select(".job_tag, .tag_wrap, [class*='tag']"):
                skill_parts.append(tag_el.get_text(" ", strip=True))
            skill_text = " ".join(skill_parts)
            skills = [normalize_skill(s) for s in extract_skills_from_text(skill_text)]

            # 업종: .corp_sector 또는 company_info 섹션에서 파싱
            industry = None
            corp_sector = item.select_one(".corp_sector") or item.select_one("[class*='industry']")
            if corp_sector:
                industry = corp_sector.get_text(strip=True) or None

            # 상세 페이지는 robots.txt Disallow → relay URL 사용
            url = f"{SARAMIN_BASE}/zf_user/jobs/relay/view?rec_idx={job_id}"

            return JobItem(
                source_site="saramin",
                source_id=str(job_id),
                url=url,
                title=title,
                company_name=company,
                job_category=category,
                industry=industry,
                employment_type=employment_type,
                skills=skills,
                location=location,
                experience_min=exp_min,
                experience_max=exp_max,
                salary_min=sal_min,
                salary_max=sal_max,
                deadline_date=deadline_date,
            )
        except Exception as e:
            logger.warning(f"[saramin] 파싱 실패: {e}")
            return None

    @staticmethod
    def _normalize_employment(text: str | None) -> str | None:
        if not text:
            return None
        t = text.strip()
        if "정규" in t:
            return "정규직"
        if "계약" in t or "기간제" in t:
            return "계약직"
        if "인턴" in t:
            return "인턴"
        if "아르바이트" in t or "파트" in t:
            return "아르바이트"
        if "프리랜서" in t:
            return "프리랜서"
        return t or None

    @staticmethod
    def _parse_experience(exp_str: str | None) -> tuple[int | None, int | None]:
        if not exp_str:
            return None, None
        if "신입" in exp_str and "경력" not in exp_str:
            return 0, 0
        if "경력무관" in exp_str or "무관" in exp_str:
            return None, None
        nums = re.findall(r"\d+", exp_str)
        nums = [int(n) for n in nums]
        if len(nums) >= 2:
            return nums[0], nums[1]
        if len(nums) == 1:
            return nums[0], None
        return None, None

    @staticmethod
    def _parse_deadline(deadline_str: str | None) -> date | None:
        if not deadline_str:
            return None
        if "상시" in deadline_str or "채용" in deadline_str:
            return None
        try:
            return datetime.strptime(deadline_str, "%y.%m.%d").date()
        except ValueError:
            return None
