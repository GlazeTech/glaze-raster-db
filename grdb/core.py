from sqlalchemy import Engine, Table
from sqlmodel import SQLModel

from grdb.models import (
    PulseDB,
    RasterInfoDB,
    SchemaVersion,
)


def create_tables(engine: Engine) -> None:
    """Create the necessary tables in the database."""
    SQLModel.metadata.create_all(engine, tables=_get_tables())


def _get_tables() -> list[Table]:
    """Get the names of all tables in the database."""
    return [
        SQLModel.metadata.tables[table_name]
        for table_name in [
            RasterInfoDB.__tablename__,
            PulseDB.__tablename__,
            SchemaVersion.__tablename__,
        ]
    ]
