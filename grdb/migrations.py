from typing import Callable

from sqlalchemy import Engine, text
from sqlmodel import Session, select

from grdb.core import create_tables
from grdb.models import PulseDB, RasterInfoDB, SchemaVersion


def _migrate_to_v2(engine: Engine) -> None:
    """Migrate from version 1 (v0.1.0) to version 2 (v0.2.0)."""
    # Create the missing table RasterInfoDB
    create_tables(engine)

    # Add user_coordinates column to raster_info table
    with engine.connect() as connection:
        connection.execute(
            text(
                f"ALTER TABLE {RasterInfoDB.__tablename__} ADD COLUMN user_coordinates TEXT"
            )
        )

    with Session(engine) as session:
        # Create version record for version 2
        schema_version = SchemaVersion(version=2)
        session.add(schema_version)
        session.commit()


def _migrate_to_v3(engine: Engine) -> None:
    """Migrate from version 2 to version 3.

    - Add `variant` column to `pulses`
    - Backfill values based on `is_reference`
    - delete is_reference column
    """
    # Ensure tables exist
    create_tables(engine)

    with engine.connect() as connection:
        # Add variant column as TEXT (SQLite is lenient; ORM maps Enum -> TEXT/Check)
        connection.execute(
            text(f"ALTER TABLE {PulseDB.__tablename__} ADD COLUMN variant TEXT")
        )

        # Backfill reference vs sample
        connection.execute(
            text(
                f"UPDATE {PulseDB.__tablename__} SET variant = 'reference' WHERE is_reference = 1"  # noqa: S608
            )
        )
        connection.execute(
            text(
                f"UPDATE {PulseDB.__tablename__} SET variant = 'sample' WHERE is_reference = 0"  # noqa: S608
            )
        )
        # Drop is_reference column
        connection.execute(
            text(f"ALTER TABLE {PulseDB.__tablename__} DROP COLUMN is_reference")
        )
        connection.commit()
    # Update the existing schema version row to version 3
    with Session(engine) as session:
        version_row = session.exec(select(SchemaVersion)).first()
        if version_row is not None:
            version_row.version = 3
            session.add(version_row)
            session.commit()


MIGRATION_SCRIPTS: dict[int, Callable[[Engine], None]] = {
    1: _migrate_to_v2,
    2: _migrate_to_v3,
}
