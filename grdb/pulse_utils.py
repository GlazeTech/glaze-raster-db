from collections.abc import Sequence
from typing import Optional
from uuid import UUID

from sqlmodel import Session, select

from grdb.models import (
    BaseMeasurement,
    Measurement,
    Point3D,
    PulseCompositionTable,
    PulseCompositionView,
    PulseDB,
    RasterResult,
)


def _get_final_pulses(session: Session, offset: int, limit: int) -> list[PulseDB]:
    """Return pulses not used as a source in any composition (aka final pulses)."""
    stmt = (
        select(PulseDB)
        .where(~PulseDB.uuid.in_(select(PulseCompositionTable.source_uuid)))  # type: ignore[attr-defined]
        .offset(offset)
        .limit(limit)
    )
    return session.exec(stmt).all()


def _get_compositions_by_derived(
    session: Session, derived_ids: Sequence[UUID]
) -> dict[UUID, list[PulseCompositionTable]]:
    """Fetch composition rows for the given derived pulse UUIDs, grouped by derived UUID."""
    if not derived_ids:
        return {}
    stmt = select(PulseCompositionTable).where(
        PulseCompositionTable.derived_uuid.in_(list(derived_ids))
    )
    rows = session.exec(stmt).all()
    grouped: dict[UUID, list[PulseCompositionTable]] = {}
    for r in rows:
        grouped.setdefault(r.derived_uuid, []).append(r)
    return grouped


def _get_source_measurements(
    session: Session, source_ids: set[UUID]
) -> dict[UUID, BaseMeasurement]:
    """Load all source pulses and map them to BaseMeasurement by UUID."""
    if not source_ids:
        return {}
    stmt = select(PulseDB).where(PulseDB.uuid.in_(list(source_ids)))
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
) -> Optional[list[PulseCompositionView]]:
    """Create ordered stitching info for a derived pulse, if compositions exist."""
    comp_rows = comps_by_derived.get(derived_uuid)
    if not comp_rows:
        return None

    comp_rows_sorted = sorted(comp_rows, key=lambda r: r.position)
    stitching: list[PulseCompositionView] = [
        PulseCompositionView(
            pulse=sources_by_uuid[row.source_uuid],
            position=row.position,
            shift=row.shift,
        )
        for row in comp_rows_sorted
        if row.source_uuid in sources_by_uuid
    ]
    return stitching or None


def _pulse_db_to_raster_result(
    p: PulseDB, stitching: Optional[list[PulseCompositionView]]
) -> RasterResult:
    """Convert a PulseDB row and optional stitching details to a RasterResult."""
    return RasterResult(
        pulse=Measurement(
            uuid=p.uuid,
            timestamp=p.timestamp,
            time=PulseDB.unpack_floats(p.time),
            signal=PulseDB.unpack_floats(p.signal),
            stitching_info=stitching,
        ),
        point=Point3D(x=p.x, y=p.y, z=p.z),
        reference=p.reference,
    )

