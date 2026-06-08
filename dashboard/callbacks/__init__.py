"""콜백 등록 진입점. app.py에서 register_all(app)을 호출."""
from . import sidebar, board, refresh


def register_all(app) -> None:
    sidebar.register(app)
    board.register(app)
    refresh.register(app)
