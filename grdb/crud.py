import json
from collections.abc import Sequence
from pathlib import Path
from typing import Optional
from uuid import UUID

from sqlalchemy import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool
from sqlmodel import Session, create_engine, func, select

from grdb.core import create_tables
from grdb.migrations import MIGRATION_SCRIPTS
from grdb.models import (
    CURRENT_SCHEMA_VERSION,
    BaseMeasurement,
    DeviceMetadata,
    KVPair,
    Measurement,
    Point3D,
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

        # Persist composition info for any stitched reference pulses
        _persist_pulse_compositions(session, references)


def add_pulses(
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

        # Persist composition info for any stitched sample pulses
        _persist_pulse_compositions(session, pulses)


def load_metadata(
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

        # Only count user-facing (final) pulses, excluding any used as sources
        final_filter = ~PulseDB.uuid.in_(select(PulseCompositionTable.source_uuid))  # type: ignore[attr-defined]
        n_reference_pulses = session.exec(
            select(func.count()).where(final_filter, PulseDB.is_reference == True)  # noqa: E712
        ).one()
        n_sample_pulses = session.exec(
            select(func.count()).where(final_filter, PulseDB.is_reference == False)  # noqa: E712
        ).one()

        return (
            info.to_raster_config(),
            info.to_device_metadata(),
            info.to_raster_metadata(),
            n_reference_pulses,
            n_sample_pulses,
        )


def load_pulses(
    path: Path,
    offset: int,
    limit: int,
) -> tuple[list[RasterResult], list[RasterResult]]:
    """Load a batch of user-facing pulses with stitching info.

    Returns two lists of RasterResult objects: references and samples. Only
    final pulses (those not used as sources in any final pulse, e.g. through stitching) are included.
    If a pulse is stitched from components, its ``pulse.stitching_info``
    contains ordered source pulse metadata.
    """
    with Session(_make_engine(path)) as session:
        final_pulses = _get_final_pulses(session, offset=offset, limit=limit)

        final_pulse_sources = _get_final_pulse_sources(
            session, [p.uuid for p in final_pulses]
        )

        sources = _get_source_measurements(session, final_pulse_sources)

        ref_results: list[RasterResult] = []
        sample_results: list[RasterResult] = []
        for final_pulse in final_pulses:
            stitching = _build_stitching_info(
                final_pulse.uuid, final_pulse_sources, sources
            )
            result = _pulse_db_to_raster_result(final_pulse, stitching)
            (ref_results if final_pulse.is_reference else sample_results).append(result)

        return ref_results, sample_results


def add_annotations(
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


def _persist_pulse_compositions(
    session: Session, results: Sequence[RasterResult]
) -> None:
    """Persist composition metadata and source pulses for stitched results.

    For each RasterResult that has ``pulse.derived_from`` defined, ensure that:
    - Each source BaseMeasurement exists as a PulseDB row (with minimal fields)
    - A PulseCompositionTable row links the final pulse to each source with
      the recorded position and shift.
    """
    # Insert source pulses first
    for res in results:
        if not res.pulse.derived_from:
            continue
        for measurement in res.pulse.derived_from:
            session.add(PulseDB.from_base_measurement(measurement.pulse))
    session.commit()

    # Now insert composition links
    for res in results:
        if not res.pulse.derived_from:
            continue
        for measurement in res.pulse.derived_from:
            session.add(
                PulseCompositionTable(
                    final_uuid=res.pulse.uuid,
                    source_uuid=measurement.pulse.uuid,
                    position=measurement.position,
                    shift=measurement.shift,
                )
            )
    session.commit()


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


def _get_final_pulses(session: Session, offset: int, limit: int) -> Sequence[PulseDB]:
    """Return pulses not used as a source in any composition (aka final pulses)."""
    stmt = (
        select(PulseDB)
        .where(~PulseDB.uuid.in_(select(PulseCompositionTable.source_uuid)))  # type: ignore[attr-defined]
        .offset(offset)
        .limit(limit)
    )
    return session.exec(stmt).all()


def _get_final_pulse_sources(
    session: Session, derived_ids: Sequence[UUID]
) -> dict[UUID, list[PulseCompositionTable]]:
    """Fetch source rows for the given final pulse UUIDs."""
    stmt = select(PulseCompositionTable).where(
        PulseCompositionTable.final_uuid.in_(list(derived_ids))  # type: ignore[attr-defined]
    )
    rows = session.exec(stmt).all()
    grouped: dict[UUID, list[PulseCompositionTable]] = {}
    for r in rows:
        grouped.setdefault(r.final_uuid, []).append(r)
    return grouped


def _get_source_measurements(
    session: Session, final_pulse_sources: dict[UUID, list[PulseCompositionTable]]
) -> dict[UUID, BaseMeasurement]:
    """Load all source pulses and map them to BaseMeasurement by UUID."""
    source_ids: set[UUID] = set()
    for composition_rows in final_pulse_sources.values():
        for row in composition_rows:
            source_ids.add(row.source_uuid)

    if not source_ids:
        return {}

    stmt = select(PulseDB).where(PulseDB.uuid.in_(list(source_ids)))  # type: ignore[attr-defined]
    src_pulses = session.exec(stmt).all()
    return {
        sp.uuid: BaseMeasurement(
            uuid=sp.uuid,
            timestamp=sp.timestamp,
            time=PulseDB.unpack_floats(sp.time),
            signal=PulseDB.unpack_floats(sp.signal),
        )
        for sp in src_pulses
    }


def _build_stitching_info(
    derived_uuid: UUID,
    comps_by_derived: dict[UUID, list[PulseCompositionTable]],
    sources_by_uuid: dict[UUID, BaseMeasurement],
) -> Optional[list[PulseComposition]]:
    """Create ordered stitching info for a derived pulse, if compositions exist."""
    comp_rows = comps_by_derived.get(derived_uuid)
    if not comp_rows:
        return None

    comp_rows_sorted = sorted(comp_rows, key=lambda r: r.position)
    stitching: list[PulseComposition] = [
        PulseComposition(
            pulse=sources_by_uuid[row.source_uuid],
            position=row.position,
            shift=row.shift,
        )
        for row in comp_rows_sorted
        if row.source_uuid in sources_by_uuid
    ]
    return stitching or None


def _pulse_db_to_raster_result(
    p: PulseDB, stitching: Optional[list[PulseComposition]]
) -> RasterResult:
    """Convert a PulseDB row and optional stitching details to a RasterResult."""
    return RasterResult(
        pulse=Measurement(
            uuid=p.uuid,
            timestamp=p.timestamp,
            time=PulseDB.unpack_floats(p.time),
            signal=PulseDB.unpack_floats(p.signal),
            derived_from=stitching,
        ),
        point=Point3D(x=p.x, y=p.y, z=p.z),
        reference=p.reference,
    )
