import contextlib
from collections.abc import Generator
from pathlib import Path

import pytest


@pytest.fixture
def db_path(tmp_path: Path) -> Generator[Path, None, None]:
    path = tmp_path / "test.db"
    yield path
    with contextlib.suppress(FileNotFoundError):
        path.unlink()
