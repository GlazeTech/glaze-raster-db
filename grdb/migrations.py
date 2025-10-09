from collections.abc import Callable

from sqlalchemy import Engine, text
from sqlmodel import Session, select

from grdb.core import create_tables
from grdb.models import PulseDB, RasterInfoDB, SchemaVersion


def _migrate_to_v2(engine: Engine) -> None:
    """Migrate from version 1 (v0.1.0) to version 2 (v0.2.0)."""
    # Create the missing table RasterInfoDB
    create_tables(engine)

    # Add user_coordinates column to raster_info table
    statements = [
        f"ALTER TABLE {RasterInfoDB.__tablename__} ADD COLUMN user_coordinates TEXT"
    ]
    _execute_text(engine, statements)

    _update_schema_version(engine, 2)


def _migrate_to_v3(engine: Engine) -> None:
    """Migrate from version 2 to version 3.

    - Add `variant` column to `pulses`
    - Add `annotations` column to `pulses`
    - Backfill values based on `is_reference`
    - delete is_reference column
    """
    # Ensure tables exist
    create_tables(engine)
    statements = [
        f"ALTER TABLE {PulseDB.__tablename__} ADD COLUMN variant TEXT",
        f"ALTER TABLE {PulseDB.__tablename__} ADD COLUMN annotations TEXT",
        f"UPDATE {PulseDB.__tablename__} SET variant = 'reference' WHERE is_reference = 1",  # noqa: S608
        f"UPDATE {PulseDB.__tablename__} SET variant = 'sample' WHERE is_reference = 0",  # noqa: S608
        f"ALTER TABLE {PulseDB.__tablename__} DROP COLUMN is_reference",
        f"UPDATE {PulseDB.__tablename__} SET annotations = '[]' WHERE annotations IS NULL",  # noqa: S608
    ]
    _execute_text(engine, statements)

    # Update the existing schema version row to version 3
    _update_schema_version(engine, 3)


def _migrate_to_v4(engine: Engine) -> None:
    """Migrate from version 3 to version 4.

    - Add `pass_number` column to `pulses`
    - Add `repetitions_config` column to `raster_info`
    - Initialize values to NULL
    """
    # Ensure tables exist
    create_tables(engine)
    statements = [
        f"ALTER TABLE {PulseDB.__tablename__} ADD COLUMN pass_number INTEGER",
        f"ALTER TABLE {PulseDB.__tablename__} ADD COLUMN noise TEXT",
        f"ALTER TABLE {RasterInfoDB.__tablename__} ADD COLUMN repetitions_config TEXT",
    ]
    _execute_text(engine, statements)

    # Update the existing schema version row to version 4
    _update_schema_version(engine, 4)


def _update_schema_version(engine: Engine, new_version: int) -> None:
    with Session(engine) as session:
        version_row = session.exec(select(SchemaVersion)).first()
        if version_row is not None:
            version_row.version = new_version
        else:
            # Create a new schema_version row if one doesn't exist
            version_row = SchemaVersion(version=new_version)
        session.add(version_row)
        session.commit()


def _execute_text(engine: Engine, statements: list[str]) -> None:
    """Execute a list of SQL statements and commit them."""
    with engine.connect() as connection:
        for stmt in statements:
            connection.execute(text(stmt))
        connection.commit()


MIGRATION_SCRIPTS: dict[int, Callable[[Engine], None]] = {
    1: _migrate_to_v2,
    2: _migrate_to_v3,
    3: _migrate_to_v4,
}
