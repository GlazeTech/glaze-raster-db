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
from grdb.pulse_utils import (
    _build_stitching_info,
    _get_compositions_by_derived,
    _get_final_pulses,
    _get_source_measurements,
    _pulse_db_to_raster_result,
)


def create_and_save_raster_db(
    path: Path,
    raster_config: RasterConfig,
    device_metadata: DeviceMetadata,
    raster_metadata: RasterMetadata,
    references: Sequence[RasterResult],
) -> None:
    """Create a new raster DB file and seed it.

    - Creates tables and writes metadata/configuration.
    - Inserts the given reference pulses as initial data.

    Args:
        path: Destination SQLite file path.
        raster_config: Acquisition configuration.
        device_metadata: Device information.
        raster_metadata: Session metadata.
        references: Initial reference pulses to store.
    """
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
    """Append non-reference pulses to an existing raster DB.

    Args:
        path: SQLite DB file path.
        pulses: Sample pulses to insert.
    """
    with Session(_make_engine(path)) as session:
        pulse_objs = [PulseDB.from_raster_result(p, is_reference=False) for p in pulses]
        session.add_all(pulse_objs)
        session.commit()


def load_raster_metadata_from_db(
    path: Path,
) -> tuple[RasterConfig, DeviceMetadata, RasterMetadata, int, int]:
    """Load configuration, device, and session metadata from the DB.

    Returns the metadata along with counts of reference and sample pulses.

    Args:
        path: SQLite DB file path.

    Returns:
        (raster_config, device_metadata, raster_metadata, n_refs, n_samples)
    """
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
) -> tuple[Sequence[RasterResult], Sequence[RasterResult]]:
    """Load a batch of user-facing pulses with stitching info.

    Returns two lists of RasterResult objects: references and samples. Only
    final pulses (those not used as sources in any composition) are included.
    If a pulse is stitched from components, its ``pulse.stitching_info``
    contains ordered source pulse metadata.
    """
    with Session(_make_engine(path)) as session:
        pulses = _get_final_pulses(session, offset=offset, limit=limit)
        if not pulses:
            return [], []

        derived_ids = [p.uuid for p in pulses]
        comps_by_derived = _get_compositions_by_derived(session, derived_ids)
        source_ids = {
            row.source_uuid for rows in comps_by_derived.values() for row in rows
        }
        sources_by_uuid = _get_source_measurements(session, source_ids)

        ref_results: list[RasterResult] = []
        sample_results: list[RasterResult] = []
        for p in pulses:
            stitching = _build_stitching_info(p.uuid, comps_by_derived, sources_by_uuid)
            result = _pulse_db_to_raster_result(p, stitching)
            (ref_results if p.is_reference else sample_results).append(result)

        return ref_results, sample_results


def update_raster_annotations(
    path: Path,
    annotations: list[KVPair],
) -> None:
    """Replace the annotations in the DB with the provided list.

    Args:
        path: SQLite DB file path.
        annotations: New annotations to persist.
    """
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
            PulseCompositionTable.from_pulse_composition(c) for c in compositions
        ]
        session.add_all(composition_objs)
        session.commit()


def read_pulse_compositions_from_db(path: Path) -> Sequence[PulseCompositionTable]:
    """Read all pulse composition rows from the DB.

    Args:
        path: SQLite DB file path.
    """
    with Session(_make_engine(path)) as session:
        stmt = select(PulseCompositionTable)
        return session.exec(stmt).all()


def _make_engine(path: Path) -> Engine:
    """Create an Engine for the DB file and ensure schema compatibility."""
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
