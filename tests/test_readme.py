from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from glaze_cicd_utils.doctest import DoctestDep, check_markdown_file

from grdb.crud import add_pulses, create_db
from grdb.models import TraceVariant
from tests.mock import make_dummy_measurement, make_dummy_metadata

if TYPE_CHECKING:
    import pytest


def test_docfile(monkeypatch: pytest.MonkeyPatch) -> None:
    path = Path("README.md")
    check_markdown_file(path, ReadmeDep(), monkeypatch)


class ReadmeDep(DoctestDep):
    def path(self: ReadmeDep) -> Path:
        return Path("README.md")

    def setup(self: ReadmeDep, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: ARG002
        # Create the raster.grf file that README examples expect
        db_path = Path("raster.grf")

        # Clean up any existing file
        if db_path.exists():
            db_path.unlink()

        # Create database with metadata
        config, device, meta = make_dummy_metadata()
        create_db(db_path, config, device, meta)

        # Add sample pulses (enough to satisfy the README examples)
        samples = make_dummy_measurement(variant=TraceVariant.sample)
        add_pulses(db_path, samples)

        # Add reference pulses
        references = make_dummy_measurement(variant=TraceVariant.reference)
        add_pulses(db_path, references)

    def teardown(self: ReadmeDep) -> None:
        # Clean up the test database file
        db_path = Path("raster.grf")
        if db_path.exists():
            db_path.unlink()
