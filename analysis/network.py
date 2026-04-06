"""기술 스택 공동 출현 네트워크 분석 (NetworkX)."""
import sqlite3
from itertools import combinations
from collections import Counter

import networkx as nx
import pandas as pd


def build_cooccurrence_graph(
    conn: sqlite3.Connection,
    category: str | None = None,
    min_cooccur: int = 3,
) -> nx.Graph:
    """
    같은 공고에 함께 등장하는 스킬 쌍으로 그래프 구성.

    Args:
        category: None이면 전체 직군
        min_cooccur: 최소 공동 출현 횟수 (엣지 필터)
    Returns:
        G: nodes = 스킬, edges = (스킬A, 스킬B, weight=공동출현수)
    """
    where = "WHERE j.is_active = 1"
    params: list = []
    if category:
        where += " AND j.job_category = ?"
        params.append(category)

    rows = pd.read_sql_query(
        f"""
        SELECT js.job_id, js.skill_name
        FROM job_skills js
        JOIN jobs j ON js.job_id = j.id
        {where}
        """,
        conn,
        params=params,
    )

    # 공고별 스킬 집합
    job_skills: dict[int, set[str]] = (
        rows.groupby("job_id")["skill_name"]
        .apply(set)
        .to_dict()
    )

    # 공동 출현 카운트
    cooccur: Counter = Counter()
    for skills in job_skills.values():
        skill_list = sorted(skills)
        for a, b in combinations(skill_list, 2):
            cooccur[(a, b)] += 1

    # 노드 빈도 (각 스킬이 몇 개 공고에 등장했는지)
    node_freq: Counter = Counter()
    for skills in job_skills.values():
        node_freq.update(skills)

    G = nx.Graph()
    for skill, freq in node_freq.items():
        G.add_node(skill, frequency=freq)

    for (a, b), weight in cooccur.items():
        if weight >= min_cooccur:
            G.add_edge(a, b, weight=weight)

    return G


def get_top_central_skills(G: nx.Graph, top_n: int = 15) -> pd.DataFrame:
    """
    중심성 기반 주요 스킬 추출.
    Returns: columns = [skill, degree_centrality, betweenness, frequency]
    """
    if len(G.nodes) == 0:
        return pd.DataFrame(columns=["skill", "degree_centrality", "betweenness", "frequency"])

    degree_c = nx.degree_centrality(G)
    between_c = nx.betweenness_centrality(G, weight="weight")

    records = [
        {
            "skill": node,
            "degree_centrality": round(degree_c[node], 4),
            "betweenness": round(between_c[node], 4),
            "frequency": G.nodes[node].get("frequency", 0),
        }
        for node in G.nodes
    ]
    return (
        pd.DataFrame(records)
        .sort_values("degree_centrality", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )


def graph_to_plotly_traces(G: nx.Graph) -> tuple[list, list]:
    """
    Plotly scatter 형식으로 변환 (대시보드용).
    Returns: (edge_traces, node_traces) — Plotly go.Scatter 데이터
    """
    import plotly.graph_objects as go

    pos = nx.spring_layout(G, seed=42, k=0.8)

    # 엣지
    edge_x, edge_y = [], []
    for u, v in G.edges():
        x0, y0 = pos[u]
        x1, y1 = pos[v]
        edge_x += [x0, x1, None]
        edge_y += [y0, y1, None]

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        mode="lines",
        line=dict(width=0.5, color="#aaa"),
        hoverinfo="none",
        name="connections",
    )

    # 노드
    node_x = [pos[n][0] for n in G.nodes]
    node_y = [pos[n][1] for n in G.nodes]
    node_text = list(G.nodes)
    node_size = [max(8, G.nodes[n].get("frequency", 1) ** 0.6) for n in G.nodes]

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode="markers+text",
        text=node_text,
        textposition="top center",
        marker=dict(
            size=node_size,
            color=[G.degree(n) for n in G.nodes],
            colorscale="Viridis",
            showscale=True,
            colorbar=dict(title="연결 수"),
        ),
        hovertemplate="<b>%{text}</b><br>공고 수: %{marker.size}<extra></extra>",
        name="skills",
    )

    return [edge_trace], [node_trace]
