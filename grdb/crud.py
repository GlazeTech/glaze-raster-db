import json
from collections.abc import Sequence
from pathlib import Path
from typing import Optional

from sqlalchemy import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool
from sqlmodel import Session, create_engine, func, select

from grdb.core import create_tables
from grdb.migrations import MIGRATION_SCRIPTS
from grdb.models import (
    CURRENT_SCHEMA_VERSION,
    DeviceMetadata,
    KVPair,
    PulseComposition,
    PulseCompositionTable,
    PulseDB,
    RasterConfig,
    RasterInfoDB,
    RasterMetadata,
    RasterResult,
    SchemaVersion,
)


def create_and_save_raster_db(
    path: Path,
    raster_config: RasterConfig,
    device_metadata: DeviceMetadata,
    raster_metadata: RasterMetadata,
    references: Sequence[RasterResult],
) -> None:
    engine = create_engine(f"sqlite:///{path}", echo=False, poolclass=NullPool)
    create_tables(engine)

    with Session(engine) as session:
        raster_info = RasterInfoDB.from_api(
            raster_config, raster_metadata, device_metadata
        )

        session.add(SchemaVersion())
        session.add(raster_info)
        session.commit()
        session.refresh(raster_info)
        pulse_objs = [
            PulseDB.from_raster_result(result, is_reference=True)
            for result in references
        ]
        session.add_all(pulse_objs)
        session.commit()


def append_pulses_to_db(
    path: Path,
    pulses: Sequence[RasterResult],
) -> None:
    with Session(_make_engine(path)) as session:
        pulse_objs = [PulseDB.from_raster_result(p, is_reference=False) for p in pulses]
        session.add_all(pulse_objs)
        session.commit()


def load_raster_metadata_from_db(
    path: Path,
) -> tuple[RasterConfig, DeviceMetadata, RasterMetadata, int, int]:
    with Session(_make_engine(path)) as session:
        stmt = select(RasterInfoDB).limit(1)
        info = session.exec(stmt).first()

        if not info:
            msg = "No metadata found in DB"
            raise ValueError(msg)

        n_reference_pulses = session.exec(
            select(func.count()).where(PulseDB.is_reference == True)  # noqa: E712
        ).one()
        n_sample_pulses = session.exec(
            select(func.count()).where(PulseDB.is_reference == False)  # noqa: E712
        ).one()

        return (
            info.to_raster_config(),
            info.to_device_metadata(),
            info.to_raster_metadata(),
            n_reference_pulses,
            n_sample_pulses,
        )


def load_pulse_batch_from_db(
    path: Path,
    offset: int,
    limit: int,
) -> tuple[Sequence[PulseDB], Sequence[PulseDB]]:
    with Session(_make_engine(path)) as session:
        # Final pulses are those not used as a source in any composition
        stmt = (
            select(PulseDB)
            .where(~PulseDB.uuid.in_(select(PulseCompositionTable.source_uuid)))  # type: ignore[attr-defined]
            .offset(offset)
            .limit(limit)
        )
        pulses = session.exec(stmt).all()
        refs = [p for p in pulses if p.is_reference]
        samples = [p for p in pulses if not p.is_reference]
        return refs, samples


def update_raster_annotations(
    path: Path,
    annotations: list[KVPair],
) -> None:
    with Session(_make_engine(path)) as session:
        stmt = select(RasterInfoDB).limit(1)
        info = session.exec(stmt).first()
        if not info:
            msg = "No metadata found in DB"
            raise ValueError(msg)

        info.annotations = json.dumps([a.model_dump() for a in annotations])
        session.add(info)
        session.commit()


def add_pulse_compositions_to_db(
    path: Path,
    compositions: Sequence[PulseComposition],
) -> None:
    """Append multiple composition groups efficiently."""
    with Session(_make_engine(path)) as session:
        composition_objs = [
            PulseCompositionTable(
                derived_uuid=c.derived_uuid,
                source_uuid=c.source_uuid,
                position=c.position,
            )
            for c in compositions
        ]
        session.add_all(composition_objs)
        session.commit()


def read_pulse_compositions_from_db(path: Path) -> Sequence[PulseCompositionTable]:
    with Session(_make_engine(path)) as session:
        stmt = select(PulseCompositionTable)
        return session.exec(stmt).all()


def _make_engine(path: Path) -> Engine:
    if not path.exists():
        msg = f"File '{path}' does not exist"
        raise FileNotFoundError(msg)

    engine = create_engine(f"sqlite:///{path}", echo=False, poolclass=NullPool)
    _ensure_schema_compatibility(engine)
    return engine


def _ensure_schema_compatibility(engine: Engine) -> None:
    """Ensure the database schema is compatible with the current version."""
    # get current schema version
    schema_version = _get_schema_version(engine)
    if schema_version is None:
        # No schema_version table exists for v0.1.0, corresponding to schema version 1
        schema_version = 1

    while schema_version < CURRENT_SCHEMA_VERSION:
        MIGRATION_SCRIPTS[schema_version](engine)
        schema_version += 1


def _get_schema_version(engine: Engine) -> Optional[int]:
    """Get the current schema version from the database."""
    with Session(engine) as session:
        try:
            version = session.exec(select(SchemaVersion)).first()
        except OperationalError:  # No schema_version table exists in older DBs
            return None
        else:
            return version.version if version else None
