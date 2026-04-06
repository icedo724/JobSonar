"""공통 크롤러 베이스 클래스."""
import time
import random
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# 크롤링 대상 직군 키워드 (URL 파라미터용)
TARGET_CATEGORIES = {
    "데이터 엔지니어": ["데이터 엔지니어", "data engineer"],
    "데이터 분석가": ["데이터 분석", "data analyst", "데이터 분석가"],
    "ML 엔지니어": ["머신러닝", "machine learning", "ML engineer"],
    "데이터 사이언티스트": ["데이터 사이언티스트", "data scientist"],
}

# 정규화된 스킬 사전 (원문 → 표준명)
SKILL_ALIASES: dict[str, str] = {
    # 언어
    "python": "Python", "파이썬": "Python",
    "r": "R",
    "scala": "Scala",
    "java": "Java", "자바": "Java",
    "javascript": "JavaScript", "js": "JavaScript",
    "typescript": "TypeScript", "ts": "TypeScript",
    "go": "Go", "golang": "Go",
    "c++": "C++", "cpp": "C++",
    "c#": "C#",
    "rust": "Rust",
    "julia": "Julia",
    "matlab": "MATLAB",

    # DB / SQL
    "sql": "SQL",
    "mysql": "MySQL",
    "postgresql": "PostgreSQL", "postgres": "PostgreSQL",
    "oracle": "Oracle",
    "mssql": "MSSQL", "sql server": "MSSQL",
    "sqlite": "SQLite",
    "mongodb": "MongoDB", "mongo": "MongoDB",
    "cassandra": "Cassandra",
    "hbase": "HBase",
    "neo4j": "Neo4j",
    "dynamodb": "DynamoDB",
    "redis": "Redis",
    "hive": "Hive",
    "presto": "Presto", "trino": "Trino",
    "athena": "AWS Athena",

    # 클라우드
    "aws": "AWS", "amazon web services": "AWS",
    "gcp": "GCP", "google cloud": "GCP", "google cloud platform": "GCP",
    "azure": "Azure", "microsoft azure": "Azure",

    # 빅데이터 / 파이프라인
    "spark": "Apache Spark", "apache spark": "Apache Spark", "pyspark": "Apache Spark",
    "hadoop": "Hadoop",
    "airflow": "Apache Airflow", "apache airflow": "Apache Airflow",
    "kafka": "Apache Kafka", "apache kafka": "Apache Kafka",
    "flink": "Apache Flink", "apache flink": "Apache Flink",
    "nifi": "Apache NiFi",
    "databricks": "Databricks",
    "dbt": "dbt", "data build tool": "dbt",
    "luigi": "Luigi",
    "celery": "Celery",

    # 컨테이너 / 인프라
    "docker": "Docker",
    "kubernetes": "Kubernetes", "k8s": "Kubernetes",
    "terraform": "Terraform",
    "ansible": "Ansible",
    "jenkins": "Jenkins",
    "github actions": "GitHub Actions",
    "gitlab ci": "GitLab CI",
    "git": "Git", "github": "Git", "gitlab": "Git",
    "linux": "Linux", "ubuntu": "Linux",

    # ML / DL 프레임워크
    "tensorflow": "TensorFlow", "tf": "TensorFlow",
    "pytorch": "PyTorch", "torch": "PyTorch",
    "keras": "Keras",
    "scikit-learn": "scikit-learn", "sklearn": "scikit-learn",
    "xgboost": "XGBoost",
    "lightgbm": "LightGBM",
    "catboost": "CatBoost",
    "hugging face": "Hugging Face", "huggingface": "Hugging Face",
    "langchain": "LangChain",
    "openai": "OpenAI API",
    "llm": "LLM",
    "rag": "RAG",
    "mlflow": "MLflow",
    "kubeflow": "Kubeflow",
    "sagemaker": "SageMaker", "aws sagemaker": "SageMaker",
    "vertex ai": "Vertex AI",

    # 분석 / 시각화
    "pandas": "pandas",
    "numpy": "NumPy",
    "scipy": "SciPy",
    "matplotlib": "Matplotlib",
    "seaborn": "Seaborn",
    "plotly": "Plotly",
    "tableau": "Tableau", "타블로": "Tableau",
    "power bi": "Power BI", "powerbi": "Power BI",
    "looker": "Looker", "looker studio": "Looker",
    "redash": "Redash",
    "superset": "Apache Superset", "apache superset": "Apache Superset",
    "metabase": "Metabase",
    "grafana": "Grafana",
    "excel": "Excel", "엑셀": "Excel",

    # 데이터 웨어하우스
    "bigquery": "BigQuery", "bq": "BigQuery",
    "redshift": "Redshift", "aws redshift": "Redshift",
    "snowflake": "Snowflake",
    "clickhouse": "ClickHouse",

    # 검색 / 로그
    "elasticsearch": "Elasticsearch", "elastic": "Elasticsearch",
    "kibana": "Kibana",
    "logstash": "Logstash",

    # 협업 / 기타
    "jira": "Jira",
    "confluence": "Confluence",
    "notion": "Notion",
    "slack": "Slack",
    "figma": "Figma",
}


def normalize_skill(raw: str) -> str:
    """스킬 문자열을 표준명으로 변환."""
    key = raw.lower().strip()
    return SKILL_ALIASES.get(key, raw.strip())


def extract_skills_from_text(text: str) -> list[str]:
    """텍스트에서 알려진 스킬 키워드를 추출. 단일 문자 키워드는 단어 경계 적용."""
    import re
    text_lower = text.lower()
    found = set()
    for alias, canonical in SKILL_ALIASES.items():
        if len(alias) <= 2:
            # 'r', 'R' 등 짧은 키워드는 단어 경계 매칭
            if re.search(rf"\b{re.escape(alias)}\b", text_lower):
                found.add(canonical)
        else:
            if alias in text_lower:
                found.add(canonical)
    return sorted(found)


@dataclass
class JobItem:
    """크롤러가 반환하는 공고 단위."""
    source_site: str
    source_id: str
    url: str
    title: str
    company_name: str
    job_category: str
    skills: list[str] = field(default_factory=list)
    industry: str | None = None        # 회사 업종
    employment_type: str | None = None # 정규직 | 계약직 | 인턴 등
    location: str | None = None
    experience_min: int | None = None
    experience_max: int | None = None
    salary_min: int | None = None
    salary_max: int | None = None
    posted_date: date | None = None
    deadline_date: date | None = None

    def to_db_dict(self) -> dict:
        return {
            "source_site": self.source_site,
            "source_id": self.source_id,
            "url": self.url,
            "title": self.title,
            "company_name": self.company_name,
            "job_category": self.job_category,
            "industry": self.industry,
            "employment_type": self.employment_type,
            "location": self.location,
            "experience_min": self.experience_min,
            "experience_max": self.experience_max,
            "salary_min": self.salary_min,
            "salary_max": self.salary_max,
            "posted_date": self.posted_date.isoformat() if self.posted_date else None,
            "deadline_date": self.deadline_date.isoformat() if self.deadline_date else None,
        }


class BaseCrawler(ABC):
    """모든 크롤러의 공통 인터페이스."""

    SITE_NAME: str = ""
    MIN_DELAY: float = 1.5   # 요청 간 최소 딜레이 (초)
    MAX_DELAY: float = 3.5

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
        })

    def _sleep(self) -> None:
        delay = random.uniform(self.MIN_DELAY, self.MAX_DELAY)
        logger.debug(f"[{self.SITE_NAME}] sleeping {delay:.1f}s")
        time.sleep(delay)

    def _get(self, url: str, **kwargs) -> requests.Response:
        self._sleep()
        resp = self.session.get(url, timeout=15, **kwargs)
        resp.raise_for_status()
        return resp

    def _soup(self, url: str, **kwargs) -> BeautifulSoup:
        resp = self._get(url, **kwargs)
        return BeautifulSoup(resp.text, "lxml")

    @abstractmethod
    def crawl(self, category: str, max_pages: int = 10) -> list[JobItem]:
        """주어진 직군 카테고리의 공고를 수집해 반환."""
        ...
