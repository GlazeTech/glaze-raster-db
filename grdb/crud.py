from __future__ import annotations

import json
from typing import TYPE_CHECKING

from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool
from sqlmodel import Session, create_engine, func, select

from grdb.core import create_tables
from grdb.migrations import MIGRATION_SCRIPTS
from grdb.models import (
    CURRENT_SCHEMA_VERSION,
    BaseTrace,
    DeviceMetadata,
    KVPair,
    Measurement,
    PulseComposition,
    PulseCompositionTable,
    PulseDB,
    RasterConfig,
    RasterInfoDB,
    RasterMetadata,
    SchemaVersion,
    Trace,
    TraceVariant,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path
    from uuid import UUID

    from sqlalchemy import Engine


def create_db(
    path: Path,
    device_metadata: DeviceMetadata,
    raster_metadata: RasterMetadata,
    raster_config: RasterConfig | None = None,
) -> None:
    """Create a new raster DB file and save metadata.

    - Creates tables and writes metadata/configuration only. Pulses are not
      inserted at creation time; use ``add_pulses`` afterwards to add any
      measurements (including references).

    Args:
        path: Destination SQLite file path.
        device_metadata: Device information.
        raster_metadata: Session metadata (includes variant field).
        raster_config: Acquisition configuration (required for raster variant).
    """
    engine = create_engine(f"sqlite:///{path}", echo=False, poolclass=NullPool)
    create_tables(engine)

    with Session(engine) as session:
        raster_info = RasterInfoDB.from_api(
            raster_metadata,
            device_metadata,
            raster_config,
        )

        session.add(SchemaVersion())
        session.add(raster_info)
        session.commit()
        session.refresh(raster_info)
        session.commit()


def add_pulses(
    path: Path,
    pulses: Sequence[Measurement],
) -> None:
    """Append pulses to an existing raster DB.

    Args:
        path: SQLite DB file path.
        pulses: Measurements to insert (any variant).
    """
    with Session(_make_engine(path)) as session:
        pulse_objs = [PulseDB.from_measurement(p) for p in pulses]
        session.add_all(pulse_objs)

        # Persist composition info for any stitched sample pulses
        for measurement in pulses:
            _maybe_add_stitched(session, measurement.pulse)
            _maybe_add_averaged(session, measurement.pulse)

        session.commit()


def load_metadata(
    path: Path,
) -> tuple[RasterConfig | None, DeviceMetadata, RasterMetadata, int, int]:
    """Load configuration, device, and session metadata from the DB.

    Returns the metadata along with counts of reference and sample pulses.
    raster_config will be None for collection-type datasets.

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
            select(func.count()).where(final_filter, PulseDB.variant == "reference")
        ).one()
        n_sample_pulses = session.exec(
            select(func.count()).where(final_filter, PulseDB.variant == "sample")
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
    variant: TraceVariant | None = None,
) -> list[Measurement]:
    """Load a batch of user-facing pulses with composition info.

    Returns a list of Measurement objects. Only final pulses (those not used
    as sources in any final pulse, e.g., through stitching or averaging) are included.
    If a pulse is stitched from components, its ``pulse.derived_from``
    contains ordered source pulse metadata. If a pulse is averaged from sources,
    its ``pulse.averaged_from`` contains the source traces. If ``variant`` is provided,
    only pulses of that variant are returned; otherwise, all variants are returned.
    """
    with Session(_make_engine(path)) as session:
        final_pulse_rows = _get_final_pulse_rows(
            session, offset=offset, limit=limit, variant=variant
        )

        final_pulse_compositions = _get_pulse_compositions(
            session, [p.uuid for p in final_pulse_rows]
        )

        sources, all_compositions = _load_all_sources(session, final_pulse_compositions)

        results: list[Measurement] = []
        for final_pulse in final_pulse_rows:
            stitching = _maybe_build_stitching_info(
                final_pulse.uuid, all_compositions, sources
            )
            averaging = _maybe_build_averaging_info(
                final_pulse.uuid, all_compositions, sources
            )
            results.append(final_pulse.to_measurement(stitching, averaging))

        return results


def update_annotations(
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


def update_references(
    path: Path,
    measurement_ids: Sequence[UUID],
    reference_uuid: UUID | None,
) -> None:
    """Update the reference UUID for the given measurements.

    Args:
        path: SQLite DB file path.
        measurement_ids: Pulse UUIDs whose ``reference`` field should be updated.
        reference_uuid: Reference UUID to assign (use None to clear).
    """
    if not measurement_ids:
        return

    with Session(_make_engine(path)) as session:
        pulse_ids = list(measurement_ids)
        pulses = session.exec(
            select(PulseDB).where(PulseDB.uuid.in_(pulse_ids))  # type: ignore[attr-defined]
        ).all()

        found_ids = {pulse.uuid for pulse in pulses}
        missing_ids = set(pulse_ids) - found_ids
        if missing_ids:
            missing_str = ", ".join(str(missing) for missing in missing_ids)
            msg = f"Pulse UUIDs not found: {missing_str}"
            raise ValueError(msg)

        if reference_uuid is not None:
            ref_exists = session.exec(
                select(PulseDB.uuid).where(PulseDB.uuid == reference_uuid)
            ).first()
            if ref_exists is None:
                msg = f"Reference UUID not found: {reference_uuid}"
                raise ValueError(msg)

        for pulse in pulses:
            pulse.reference = reference_uuid
            session.add(pulse)

        session.commit()


def _maybe_add_stitched(session: Session, trace: Trace) -> None:
    """Persist compositions for stitched pulses.

    If the trace has derived_from (stitching info), persists each source pulse
    and its composition metadata.
    """
    if trace.derived_from:
        for comp in trace.derived_from:
            session.add(PulseDB.from_basetrace(comp.pulse))
            session.add(
                PulseCompositionTable(
                    final_uuid=trace.uuid,
                    source_uuid=comp.pulse.uuid,
                    position=comp.position,
                    shift=comp.shift,
                    composition_type="stitch",
                )
            )


def _maybe_add_averaged(session: Session, trace: Trace) -> None:
    """Persist compositions for averaged pulses.

    If the trace has averaged_from, persists each source trace and its composition
    metadata. Averaged sources can be stitched, but cannot be averaged (no nested
    averaging allowed).
    """
    if trace.averaged_from:
        for source_trace in trace.averaged_from:
            session.add(PulseDB.from_basetrace(source_trace))
            session.add(
                PulseCompositionTable(
                    final_uuid=trace.uuid,
                    source_uuid=source_trace.uuid,
                    position=None,
                    shift=None,
                    composition_type="average",
                )
            )

            # An averaged pulse can consist of multiple stitched sources;
            # recursively persist compositions of this nested source
            _maybe_add_stitched(session, source_trace)


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


def _get_schema_version(engine: Engine) -> int | None:
    """Get the current schema version from the database."""
    with Session(engine) as session:
        try:
            version = session.exec(select(SchemaVersion)).first()
        except OperationalError:  # No schema_version table exists in older DBs
            return None
        else:
            return version.version if version else None


def _get_final_pulse_rows(
    session: Session, offset: int, limit: int, variant: TraceVariant | None = None
) -> Sequence[PulseDB]:
    """Return pulses not used as a source in any composition (aka final pulses).

    If ``variant`` is provided, restrict to pulses of that variant.
    """
    stmt = select(PulseDB).where(
        ~PulseDB.uuid.in_(select(PulseCompositionTable.source_uuid))  # type: ignore[attr-defined]
    )
    if variant is not None:
        stmt = stmt.where(PulseDB.variant == variant)
    stmt = stmt.offset(offset).limit(limit)
    return session.exec(stmt).all()


def _get_pulse_compositions(
    session: Session, final_ids: Sequence[UUID]
) -> dict[UUID, list[PulseCompositionTable]]:
    """Fetch source rows for the given final pulse UUIDs."""
    stmt = select(PulseCompositionTable).where(
        PulseCompositionTable.final_uuid.in_(list(final_ids))  # type: ignore[attr-defined]
    )
    rows = session.exec(stmt).all()
    grouped: dict[UUID, list[PulseCompositionTable]] = {}
    for r in rows:
        grouped.setdefault(r.final_uuid, []).append(r)
    return grouped


def _extract_unprocessed_source_uuids(
    compositions: dict[UUID, list[PulseCompositionTable]],
    processed_uuids: set[UUID],
) -> list[UUID]:
    """Extract source UUIDs from compositions that haven't been processed yet."""
    unprocessed: list[UUID] = []
    for comp_list in compositions.values():
        for comp_row in comp_list:
            if comp_row.source_uuid not in processed_uuids:
                unprocessed.append(comp_row.source_uuid)  # noqa: PERF401
    return unprocessed


def _load_all_sources(
    session: Session,
    pulse_sources: dict[UUID, list[PulseCompositionTable]],
) -> tuple[dict[UUID, BaseTrace], dict[UUID, list[PulseCompositionTable]]]:
    """Load all source pulses and their compositions recursively.

    Returns (sources, all_compositions) where sources maps UUID -> BaseTrace
    and all_compositions maps UUID -> list of composition rows.
    """
    all_sources: dict[UUID, BaseTrace] = {}
    all_compositions = dict(pulse_sources)
    processed_uuids: set[UUID] = set()

    # Get initial source UUIDs from final pulse compositions (all unprocessed)
    to_load = [
        comp_row.source_uuid
        for comp_list in pulse_sources.values()
        for comp_row in comp_list
    ]

    # Recursively load source pulses and source pulse's compositions
    while to_load:
        current_batch = to_load
        to_load = []

        # Load pulse data for current batch
        stmt = select(PulseDB).where(PulseDB.uuid.in_(current_batch))  # type: ignore[attr-defined]
        pulses = session.exec(stmt).all()
        for pulse in pulses:
            all_sources[pulse.uuid] = pulse.to_basetrace()
            processed_uuids.add(pulse.uuid)

        # Load composition info for current batch
        batch_comps = _get_pulse_compositions(session, current_batch)
        all_compositions.update(batch_comps)

        # Add newly discovered sources to the queue
        newly_discovered = _extract_unprocessed_source_uuids(
            batch_comps, processed_uuids
        )
        to_load.extend(newly_discovered)

    return all_sources, all_compositions


def _maybe_build_stitching_info(
    derived_uuid: UUID,
    all_compositions: dict[UUID, list[PulseCompositionTable]],
    sources: dict[UUID, BaseTrace],
) -> list[PulseComposition] | None:
    """Create ordered stitching info for a derived pulse, if stitch compositions exist."""
    comp_rows = all_compositions.get(derived_uuid)
    if not comp_rows:
        return None

    # Filter for stitch-type compositions only
    stitch_rows = [r for r in comp_rows if r.composition_type == "stitch"]
    if not stitch_rows:
        return None

    # Sort by position (guaranteed non-None for stitch type by check constraint)
    stitch_rows_sorted = sorted(stitch_rows, key=lambda r: r.position or 0)
    stitching: list[PulseComposition] = [
        PulseComposition(
            pulse=sources[row.source_uuid],
            position=row.position,
            shift=row.shift,
        )
        for row in stitch_rows_sorted
    ]
    return stitching or None


def _maybe_build_averaging_info(
    derived_uuid: UUID,
    all_compositions: dict[UUID, list[PulseCompositionTable]],
    sources: dict[UUID, BaseTrace],
) -> list[Trace] | None:
    """Create averaging info for a derived pulse, if average compositions exist.

    Note: Averaged sources can be stitched, but cannot themselves be averaged
    (no nested averaging allowed).
    """
    comp_rows = all_compositions.get(derived_uuid)
    if not comp_rows:
        return None

    # Filter for average-type compositions only
    average_rows = [r for r in comp_rows if r.composition_type == "average"]
    if not average_rows:
        return None

    # Build Trace objects for averaged sources
    # Note: averaged sources can be stitched, but cannot be averaged (no nested averaging)
    averaging: list[Trace] = []
    for row in average_rows:
        if row.source_uuid not in sources:
            continue

        # Check if this averaged source is itself stitched
        source_stitching = _maybe_build_stitching_info(
            row.source_uuid, all_compositions, sources
        )

        averaging.append(
            Trace(
                time=sources[row.source_uuid].time,
                signal=sources[row.source_uuid].signal,
                uuid=sources[row.source_uuid].uuid,
                timestamp=sources[row.source_uuid].timestamp,
                noise=sources[row.source_uuid].noise,
                derived_from=source_stitching,
                averaged_from=None,  # No nested averaging allowed
            )
        )

    return averaging or None
