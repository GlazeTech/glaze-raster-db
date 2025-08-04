from typing import Callable

from sqlalchemy import Engine, text
from sqlmodel import Session

from grdb.core import create_tables
from grdb.models import RasterInfoDB, SchemaVersion


def _migrate_to_v2(engine: Engine) -> None:
    """Migrate from version 1 (v0.1.0) to version 2 (v0.2.0)."""
    # Create the missing table RasterInfoDB
    create_tables(engine)

    # Add working_coordinates column to raster_info table
    with engine.connect() as connection:
        connection.execute(
            text(
                f"ALTER TABLE {RasterInfoDB.__tablename__} ADD COLUMN working_coordinates TEXT"
            )
        )

    with Session(engine) as session:
        # Create version record for version 2
        schema_version = SchemaVersion(version=2)
        session.add(schema_version)
        session.commit()


MIGRATION_SCRIPTS: dict[int, Callable[[Engine], None]] = {
    1: _migrate_to_v2,
}
