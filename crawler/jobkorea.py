"""잡코리아 크롤러 — robots.txt Allow 확인 완료 (목록·상세 모두 허용)."""
import logging
import re
from datetime import date, datetime

from bs4 import BeautifulSoup, Tag

from .base import BaseCrawler, JobItem, normalize_skill, extract_skills_from_text

logger = logging.getLogger(__name__)

JOBKOREA_KEYWORDS: dict[str, str] = {
    "데이터 엔지니어": "데이터 엔지니어",
    "데이터 분석가": "데이터 분석가",
    "데이터 사이언티스트": "데이터 사이언티스트",
    "ML 엔지니어": "머신러닝 엔지니어",
}

JOBKOREA_BASE    = "https://www.jobkorea.co.kr"
JOBKOREA_SEARCH  = f"{JOBKOREA_BASE}/Search/"
JOBKOREA_JOB_URL = f"{JOBKOREA_BASE}/Recruit/GI_Read/{{job_id}}"


def _parse_experience_jk(text: str | None) -> tuple[int | None, int | None]:
    """'경력7년↑', '경력3~5년', '신입' 등 파싱."""
    if not text:
        return None, None
    if "경력무관" in text or ("신입" in text and "경력" in text):
        return None, None
    if "신입" in text:
        return 0, 0
    nums = re.findall(r"\d+", text)
    nums = [int(n) for n in nums]
    if len(nums) >= 2:
        return nums[0], nums[1]
    if len(nums) == 1:
        return nums[0], None
    return None, None


def _normalize_employment_jk(text: str | None) -> str | None:
    if not text:
        return None
    t = text.strip()
    if t in ("정규직",):
        return "정규직"
    if t in ("계약직", "기간제"):
        return "계약직"
    if t in ("인턴",):
        return "인턴"
    if t in ("아르바이트",):
        return "아르바이트"
    if t in ("프리랜서",):
        return "프리랜서"
    return None


def _parse_deadline_jk(text: str | None) -> date | None:
    """'05/01(금) 마감', '상시채용' 파싱."""
    if not text or "상시" in text or "채용" in text:
        return None
    m = re.search(r"(\d{2})/(\d{2})", text)
    if not m:
        return None
    try:
        year = datetime.now().year
        return date(year, int(m.group(1)), int(m.group(2)))
    except ValueError:
        return None


class JobKoreaCrawler(BaseCrawler):
    SITE_NAME = "jobkorea"
    MIN_DELAY = 2.0
    MAX_DELAY = 4.0

    def crawl(self, category: str, max_pages: int = 10) -> list[JobItem]:
        keyword = JOBKOREA_KEYWORDS.get(category)
        if keyword is None:
            raise ValueError(f"Unknown category: {category}")

        jobs: list[JobItem] = []
        seen_ids: set[str] = set()

        for page in range(1, max_pages + 1):
            logger.info(f"[jobkorea] {category} — page {page}")

            try:
                soup = self._soup(
                    JOBKOREA_SEARCH,
                    params={"stext": keyword, "tabType": "recruit", "Page_No": page},
                )
            except Exception as e:
                logger.error(f"[jobkorea] 목록 오류 (page={page}): {e}")
                break

            # GI_Read 링크 기준으로 공고 카드 수집 (CSS 클래스 해시 변경에 무관)
            links = soup.find_all("a", href=re.compile(r"GI_Read/\d+"))
            if not links:
                logger.info(f"[jobkorea] 더 이상 공고 없음 (page={page})")
                break

            page_new = 0
            for a in links:
                m = re.search(r"GI_Read/(\d+)", a["href"])
                if not m:
                    continue
                job_id = m.group(1)
                if job_id in seen_ids:
                    continue
                seen_ids.add(job_id)

                # 링크의 3레벨 위 div가 공고 카드
                card = a.parent.parent.parent
                job = self._parse_card(card, job_id, category)
                if job:
                    jobs.append(job)
                    page_new += 1

            if page_new == 0:
                break

        logger.info(f"[jobkorea] {category} 총 {len(jobs)}건 수집 완료")
        return jobs

    def crawl_all_categories(self, max_pages: int = 10) -> list[JobItem]:
        all_jobs: list[JobItem] = []
        for category in JOBKOREA_KEYWORDS:
            all_jobs.extend(self.crawl(category, max_pages=max_pages))
        return all_jobs

    def _parse_card(self, card: Tag, job_id: str, category: str) -> JobItem | None:
        try:
            spans = [s.get_text(strip=True) for s in card.find_all("span") if s.get_text(strip=True)]
            # '스크랩' 이후가 실제 정보 — 스크랩 인덱스 찾기
            try:
                start = spans.index("스크랩") + 1
            except ValueError:
                start = 0
            spans = spans[start:]

            # 제목 (첫 번째 의미있는 span)
            title = spans[0] if spans else ""

            # 회사명: text-typo-b2-16 클래스
            company_el = card.select_one("span.text-typo-b2-16")
            company = company_el.get_text(strip=True) if company_el else ""

            # 위치: place2 이모지 다음 span
            place_icon = card.find("span", class_=re.compile(r"basicemoji-place"))
            location = None
            if place_icon:
                sib = place_icon.find_next_sibling("span")
                location = sib.get_text(strip=True) if sib else None

            # 경력: ↑ 포함하거나 '경력' 포함 span
            exp_span = next(
                (s for s in card.find_all("span") if re.search(r"경력|신입", s.get_text())),
                None,
            )
            exp_min, exp_max = _parse_experience_jk(exp_span.get_text(strip=True) if exp_span else None)

            # 마감일: 마지막 span들에서 '마감' 텍스트
            deadline_span = next(
                (s for s in reversed(card.find_all("span")) if "마감" in s.get_text()),
                None,
            )
            deadline_date = _parse_deadline_jk(deadline_span.get_text(strip=True) if deadline_span else None)

            # 스킬: 제목 + 카드 내 모든 텍스트에서 추출
            card_text = title + " " + card.get_text(" ", strip=True)
            skills = [normalize_skill(s) for s in extract_skills_from_text(card_text)]

            # 업종·근무형태: span 탐색
            industry = None
            employment_type = None
            for span in card.find_all("span"):
                cls = " ".join(span.get("class", []))
                txt = span.get_text(strip=True)
                if not txt:
                    continue
                if any(k in cls for k in ["industry", "sector", "업종"]):
                    industry = txt
                if any(k in cls for k in ["employment", "jobtype", "고용"]):
                    employment_type = _normalize_employment_jk(txt)
            # 근무형태가 없으면 spans 텍스트에서 패턴 매칭
            if not employment_type:
                for s in card.find_all("span"):
                    t = s.get_text(strip=True)
                    emp = _normalize_employment_jk(t)
                    if emp:
                        employment_type = emp
                        break

            return JobItem(
                source_site="jobkorea",
                source_id=job_id,
                url=JOBKOREA_JOB_URL.format(job_id=job_id),
                title=title,
                company_name=company,
                job_category=category,
                industry=industry,
                employment_type=employment_type,
                skills=skills,
                location=location,
                experience_min=exp_min,
                experience_max=exp_max,
                deadline_date=deadline_date,
            )
        except Exception as e:
            logger.warning(f"[jobkorea] 파싱 실패 job_id={job_id}: {e}")
            return None
