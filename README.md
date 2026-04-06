---
title: JobSonar
emoji: 📡
colorFrom: blue
colorTo: indigo
sdk: docker
pinned: false
license: mit
---

# 📡 JobSonar

한국 IT 채용 시장 트렌드를 자동 수집·분석하는 실시간 대시보드.

> 데이터 분석가 포트폴리오 프로젝트 — 원티드·사람인 공고를 매일 수집해 시각화합니다.

## 주요 기능

| 탭 | 내용 |
|----|------|
| 📋 공고 목록 | 키워드·지역·경력 필터, 원본 링크 |
| 📈 트렌드 | 주별 공고 수 추이, 스킬 트렌드, 경력 요건 분포 |
| 🔧 기술 스택 | 직군별 TOP 스킬, 최근 2주 급상승 스킬 |
| 🕸️ 스킬 네트워크 | 공동 출현 기반 기술 연결 그래프 |
| 💰 연봉 분석 | 직군별 박스플롯·통계·히스토그램 |
| 🏢 기업 분석 | 채용 TOP 기업, 지역별·직군×지역 히트맵 |

## 기술 스택

- **수집**: Python · requests · BeautifulSoup
- **저장**: SQLite
- **분석**: pandas · networkx
- **시각화**: Plotly · Streamlit
- **자동화**: GitHub Actions (매일 오전 10시 KST)
- **배포**: Hugging Face Spaces

## 아키텍처

```
GitHub Actions (매일)
  └─ 원티드 API / 사람인 HTML 파싱
  └─ SQLite DB 업데이트
  └─ HF Dataset({username}/jobsonar-data)에 DB 업로드

HF Space 앱 시작 시
  └─ HF Dataset에서 jobsonar.db 다운로드
  └─ Streamlit 대시보드 렌더링
```

## 로컬 실행

```bash
pip install -r requirements.txt

# 크롤링 (테스트: 원티드 3페이지)
python -m crawler.run --source wanted --max-pages 3

# 대시보드 실행 → http://localhost:8050
python dashboard/app.py
```

## 배포 (Hugging Face Spaces)

### 1. HF repo 2개 생성

| repo | 종류 |
|------|------|
| `{username}/jobsonar` | Space (Streamlit) |
| `{username}/jobsonar-data` | Dataset |

### 2. GitHub Secrets 등록

```
HF_TOKEN        → HF Write 권한 Access Token
HF_DATASET_REPO → {username}/jobsonar-data
```

### 3. Space 환경변수 등록

```
HF_DATASET_REPO → {username}/jobsonar-data
```

### 4. Space에 코드 push

```bash
git remote add hf https://huggingface.co/spaces/{username}/jobsonar
git push hf main
```

## 수집 대상 직군

- 데이터 엔지니어
- 데이터 분석가
- 데이터 사이언티스트
- ML 엔지니어

## 크롤링 정책

| 사이트 | robots.txt | 방식 |
|--------|-----------|------|
| 원티드 | 403 반환 (제한 없음으로 간주) | 공개 REST API |
| 사람인 | 상세 페이지 Disallow | 목록 페이지만 수집 |

요청 딜레이 1.5~4초 적용.
