import json
from collections.abc import Sequence
from pathlib import Path

from sqlalchemy import Engine
from sqlmodel import Session, SQLModel, create_engine, func, select

from grdb.models import (
    DeviceMetadata,
    KVPair,
    PulseDB,
    RasterConfig,
    RasterInfoDB,
    RasterMetadata,
    RasterResult,
)


def create_and_save_raster_db(
    path: Path,
    raster_config: RasterConfig,
    device_metadata: DeviceMetadata,
    raster_metadata: RasterMetadata,
    references: Sequence[RasterResult],
) -> None:
    engine = create_engine(f"sqlite:///{path}", echo=False)
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        raster_info = RasterInfoDB.from_api(
            raster_config, raster_metadata, device_metadata
        )
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
        stmt = select(PulseDB).offset(offset).limit(limit)
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


def _make_engine(path: Path) -> Engine:
    if not path.exists():
        msg = f"File '{path}' does not exist"
        raise FileNotFoundError(msg)

    return create_engine(f"sqlite:///{path}", echo=False)
