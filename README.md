# JobSonar

> 다양한 취업 플랫폼에 올라오는 데이터 관련 직군의 공고들을 모아서 보여주는 기능을 제공합니다.

**[JobSonar](https://huggingface.co/spaces/mininiming/jobsonar)**
---

## 주요 기능

| 탭 | 내용 |
|----|------|
| 공고 목록 | 키워드·지역·경력·근무형태 필터, 원본 링크 |
| 트렌드 | 주별 공고 수 추이, 스킬 트렌드, 경력 요건 분포 |
| 기술 스택 | 직군별 TOP 스킬, 최근 2주 급상승 스킬 |
| 스킬 네트워크 | 공동 출현 기반 기술 연결 그래프 |
| 연봉 분석 | 직군별 박스플롯·통계·히스토그램 |
| 기업 분석 | 채용 TOP 기업, 지역별·직군×지역 히트맵 |

---

## 기술 스택

- **수집**: Python, requests, BeautifulSoup
- **저장**: SQLite
- **분석**: pandas, networkx
- **시각화**: Plotly, Dash
- **자동화**: GitHub Actions (매일 오전 10시 KST)
- **배포**: Hugging Face Spaces (Docker)

---

## 구조

```
GitHub Actions (매일 오전 10시)
  ├─ 원티드 공개 REST API 수집
  ├─ 사람인 HTML 파싱 (목록 페이지만, robots.txt 준수)
  ├─ 잡코리아 HTML 파싱
  ├─ SQLite DB 업데이트 (upsert, 중복 공고 처리)
  └─ HF Dataset(mininiming/jobsonar-data)에 DB 업로드

HF Space 앱 시작 시
  ├─ HF Dataset에서 jobsonar.db 다운로드
  └─ Dash 대시보드 렌더링
```

---

## 로컬

```bash
pip install -r requirements.txt

# 크롤링 테스트 (원티드 3페이지)
python -m crawler.run --source wanted --max-pages 3

# 대시보드 실행
python dashboard/app.py
# → http://localhost:8050
```

---

## 수집 대상

- 데이터 엔지니어
- 데이터 분석가
- 데이터 사이언티스트
- ML 엔지니어

---

## 플렛폼 별 정책

| 사이트 | 방식 | robots.txt |
|--------|------|-----------|
| 원티드 | 공개 REST API | 문제 없음 |
| 사람인 | 목록 페이지 HTML | 상세 페이지 Disallow → relay URL 사용 |
| 잡코리아 | 목록 페이지 HTML | 허용 확인 |

요청 간 1.5~4초 딜레이 적용.

---
