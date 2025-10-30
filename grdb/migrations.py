from collections.abc import Callable
from uuid import UUID

from sqlalchemy import Engine, text
from sqlmodel import Session, select

from grdb.core import create_tables
from grdb.models import (
    PulseCompositionTable,
    PulseCompositionType,
    PulseDB,
    RasterInfoDB,
    SchemaVersion,
)


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


def _migrate_to_v5(engine: Engine) -> None:
    """Migrate from version 4 to version 5.

    - Add `composition_type` column to `pulse_composition`
    - Change `position` and `shift` columns to nullable (int -> int | None, float -> float | None)
    - Backfill all existing rows as 'stitch' (averaging is new feature)

    We must copy data to a new table due to limitations in SQLite ALTER TABLE.
    """
    # Drop old indexes (they don't get renamed with the table in SQLite)
    old_table_name = f"{PulseCompositionTable.__tablename__}_old"
    statements = [
        f"DROP INDEX IF EXISTS ix_{PulseCompositionTable.__tablename__}_final_uuid",
        f"DROP INDEX IF EXISTS ix_{PulseCompositionTable.__tablename__}_source_uuid",
        f"ALTER TABLE {PulseCompositionTable.__tablename__} RENAME TO {old_table_name}",
    ]
    _execute_text(engine, statements)

    # Create new table with updated schema using SQLModel
    create_tables(engine)

    # Copy data from old table to new, setting composition_type to 'stitch'
    with Session(engine) as session:
        old_rows = session.exec(  # type: ignore[call-overload]
            text(
                f"SELECT id, final_uuid, source_uuid, position, shift FROM {old_table_name}"  # noqa: S608
            )
        ).all()

        for row in old_rows:
            new_row = PulseCompositionTable(
                id=row.id,
                final_uuid=UUID(row.final_uuid)
                if isinstance(row.final_uuid, str)
                else row.final_uuid,
                source_uuid=UUID(row.source_uuid)
                if isinstance(row.source_uuid, str)
                else row.source_uuid,
                position=row.position,
                shift=row.shift,
                composition_type=PulseCompositionType.stitch,
            )
            session.add(new_row)
        session.commit()

    # Drop old table
    _execute_text(engine, [f"DROP TABLE {old_table_name}"])

    _update_schema_version(engine, 5)


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
    4: _migrate_to_v5,
}
