"""콜백 등록 진입점. app.py에서 register_all(app)을 호출."""
from . import sidebar, board, trend, skills, network, salary, company


def register_all(app) -> None:
    sidebar.register(app)
    board.register(app)
    trend.register(app)
    skills.register(app)
    network.register(app)
    salary.register(app)
    company.register(app)
