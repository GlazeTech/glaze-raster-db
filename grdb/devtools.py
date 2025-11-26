"""Developer tools for creating test databases and dummy data.

This module provides utilities for generating test databases with various
configurations of measurements, traces, and metadata. These tools are intended
for use by developers and external packages that need to test with grdb databases.
"""

from __future__ import annotations

import random
import time
import uuid
from typing import TYPE_CHECKING

from grdb.crud import add_pulses, create_db
from grdb.models import (
    AxesMapping,
    AxisMap,
    BaseTrace,
    CoordinateTransform,
    DeviceMetadata,
    KVPair,
    Measurement,
    Point3D,
    Point3DFullyDefined,
    PulseComposition,
    RasterConfig,
    RasterMetadata,
    RasterPattern,
    RepetitionsConfig,
    Trace,
    TraceVariant,
)

if TYPE_CHECKING:
    from pathlib import Path


def make_dummy_metadata(
    device_serial_number: str | None = None,
    raster_id: uuid.UUID | None = None,
) -> tuple[RasterConfig, DeviceMetadata, RasterMetadata]:
    """Create dummy metadata for testing.

    Args:
        device_serial_number: Optional device serial number. Defaults to "123-ABC".
        raster_id: Optional raster ID. Defaults to None.

    Returns:
        Tuple containing RasterConfig, DeviceMetadata, and RasterMetadata
        with representative test values.
    """
    config = RasterConfig(
        patterns=[
            RasterPattern(
                start_point=Point3D(x=0, y=0, z=0),
                end_point=Point3D(x=1, y=1, z=1),
            )
        ],
        stepsize=0.5,
        reference_point=Point3D(x=0, y=0, z=0),
        acquire_ref_every=2,
        repetitions_config=RepetitionsConfig(passes=3, interval_millisecs=30_000),
    )
    device = DeviceMetadata(
        device_serial_number=device_serial_number or "123-ABC",
        device_firmware_version="v1.0.0",
    )
    meta = RasterMetadata(
        variant="raster",
        app_version="app1",
        raster_id=raster_id,
        timestamp=161803398,
        annotations=[KVPair(key="foo", value="bar"), KVPair(key="baz", value=1.0)],
        device_configuration={"mode": "test"},
        user_coordinates=make_dummy_coordinate_transform(),
    )
    return (config, device, meta)


def make_dummy_coordinate_transform() -> CoordinateTransform:
    """Create a dummy coordinate transform for testing.

    Returns:
        A CoordinateTransform with sample offset, axis mapping, and metadata.
    """
    return CoordinateTransform(
        id=uuid.uuid4(),
        name="Test Coordinate System",
        offset=Point3DFullyDefined(x=10.0, y=20.0, z=30.0),
        mapping=AxesMapping(
            x=AxisMap(axis="z", sign=1),
            y=AxisMap(axis="y", sign=-1),
            z=AxisMap(axis="x", sign=1),
        ),
        last_used=int(time.time() * 1000),
        notes="Dummy coordinate transform for testing",
    )


def make_dummy_measurement(
    variant: TraceVariant,
    n_results: int = 2,
    pulse_length: int = 3,
    composed_of_n: int = 0,
    averaged_of_n: int = 0,
) -> list[Measurement]:
    """Create a list of dummy measurements.

    Args:
        variant: The trace variant (reference, sample, noise, other).
        n_results: Number of measurements to create.
        pulse_length: Length of the pulse waveform.
        composed_of_n: Number of source pulses if this is a stitched trace.
        averaged_of_n: Number of source pulses if this is an averaged trace.

    Returns:
        List of Measurement objects with dummy data.
    """
    return [
        Measurement(
            pulse=make_dummy_trace(
                pulse_length=pulse_length,
                composed_of_n=composed_of_n,
                averaged_of_n=averaged_of_n,
            ),
            point=Point3D(x=float(i), y=float(i), z=float(i)),
            reference=None,
            variant=variant,
            annotations=[],
        )
        for i in range(n_results)
    ]


def make_dummy_trace(
    pulse_length: int = 2,
    composed_of_n: int = 0,
    averaged_of_n: int = 0,
    averaged_of_composed_of_n: int = 0,
    noise: uuid.UUID | None = None,
) -> Trace:
    """Create a dummy trace with optional composition or averaging.

    Args:
        pulse_length: Length of the pulse waveform.
        composed_of_n: Number of source pulses if this is a stitched trace (>=2).
        averaged_of_n: Number of source pulses if this is an averaged trace (>=2).
        averaged_of_composed_of_n: If averaging, number of sources per averaged trace.
        noise: UUID of the noise trace, if any.

    Returns:
        A Trace object with random signal data.

    Raises:
        ValueError: If trace is both stitched and averaged, or if composition/averaging
            counts are less than 2.
    """
    min_composition_sources = 2
    if composed_of_n and averaged_of_n:
        msg = "A trace cannot be both stitched and averaged"
        raise ValueError(msg)
    if composed_of_n and composed_of_n < min_composition_sources:
        msg = "Stitched traces require at least two source pulses"
        raise ValueError(msg)
    if averaged_of_n and averaged_of_n < min_composition_sources:
        msg = "Averaged traces require at least two sources"
        raise ValueError(msg)
    composition = (
        make_dummy_composition(
            composed_of_n=composed_of_n, pulse_length=pulse_length, noise=noise
        )
        if composed_of_n > 0
        else None
    )
    averaged_sources: list[Trace] | None = None
    if averaged_of_n > 0:
        averaged_sources = [
            make_dummy_trace(
                pulse_length=pulse_length,
                noise=noise,
                composed_of_n=averaged_of_composed_of_n,
            )
            for _ in range(averaged_of_n)
        ]
    return Trace(
        time=[1.0 * i for i in range(pulse_length)],
        signal=[random.random() for _ in range(pulse_length)],  # noqa: S311
        uuid=uuid.uuid4(),
        noise=None if (composition or averaged_sources) else noise,
        timestamp=int(time.time() * 1000),  # Time in ms since Unix epoch
        derived_from=composition,
        averaged_from=averaged_sources,
    )


def make_dummy_composition(
    composed_of_n: int = 2, pulse_length: int = 2, noise: uuid.UUID | None = None
) -> list[PulseComposition]:
    """Create a list of PulseComposition objects for stitched traces.

    Args:
        composed_of_n: Number of source pulses in the composition.
        pulse_length: Length of each source pulse waveform.
        noise: UUID of the noise trace, if any.

    Returns:
        List of PulseComposition objects with position and shift metadata.
    """
    return [
        PulseComposition(
            pulse=make_dummy_basetrace(pulse_length=pulse_length, noise=noise),
            position=i,
            shift=i * 10e-12,
        )
        for i in range(composed_of_n)
    ]


def make_dummy_basetrace(
    pulse_length: int = 2, noise: uuid.UUID | None = None
) -> BaseTrace:
    """Create a dummy BaseTrace (source pulse for stitching).

    Args:
        pulse_length: Length of the pulse waveform.
        noise: UUID of the noise trace, if any.

    Returns:
        A BaseTrace object with random signal data.
    """
    return BaseTrace(
        time=[1.0 * i for i in range(pulse_length)],
        signal=[random.random() for _ in range(pulse_length)],  # noqa: S311
        uuid=uuid.uuid4(),
        timestamp=int(time.time() * 1000),  # Time in ms since Unix epoch
        noise=noise,
    )


def make_measurement_variants() -> list[Measurement]:
    """Generate a comprehensive set of Measurements covering key branches.

    This function creates measurements that exercise various code paths including:
    - Point3D: fully defined, partially defined (one None), all None
    - Annotations: mixed types, empty KV, empty list
    - Variants: reference, sample, noise, other
    - Reference field: set vs unset (sample referencing a ref)
    - Stitching/Averaging: stitched (derived_from), averaged (averaged_from), or absent
    - Pass number: None, 1, 2

    Returns:
        List of Measurement objects covering various test scenarios.
    """

    def build(  # noqa: PLR0913
        *,
        point: Point3D | None = None,
        variant: TraceVariant | None = "sample",
        annotations: list[KVPair] | None = None,
        composed_of_n: int | None = 0,
        reference_uuid: uuid.UUID | None = None,
        pass_number: int | None = None,
        noise: uuid.UUID | None = None,
        averaged_of_n: int | None = 0,
        averaged_of_n_composed_of_n: int | None = 0,
    ) -> Measurement:
        variant = variant or "sample"
        composed_of_n = composed_of_n or 0
        averaged_of_n = averaged_of_n or 0
        averaged_of_n_composed_of_n = averaged_of_n_composed_of_n or 0

        return Measurement(
            pulse=make_dummy_trace(
                composed_of_n=composed_of_n,
                averaged_of_n=averaged_of_n,
                noise=noise,
                averaged_of_composed_of_n=averaged_of_n_composed_of_n,
            ),
            point=point,
            reference=reference_uuid,
            variant=variant,
            annotations=annotations,
            pass_number=pass_number,
        )

    def build_with_potential_ref(*, with_ref: bool = False) -> list[Measurement]:
        built = []
        if with_ref:
            ref = build(variant="reference")
            ref_uuid = ref.pulse.uuid
            built.append(ref)
        else:
            ref_uuid = None
        noise = build(variant="noise")
        noise_uuid = noise.pulse.uuid
        built.extend(
            [
                noise,
                build(composed_of_n=2, reference_uuid=ref_uuid),
                build(composed_of_n=2, reference_uuid=ref_uuid, noise=noise_uuid),
                build(point=Point3D(x=1.0, y=2.0, z=3.0), reference_uuid=ref_uuid),
                build(point=Point3D(x=4.0, y=None, z=6.0), reference_uuid=ref_uuid),
                build(variant="reference", reference_uuid=ref_uuid),
                build(variant="sample", reference_uuid=ref_uuid),
                build(variant="noise", reference_uuid=ref_uuid),
                build(variant="other", reference_uuid=ref_uuid),
                build(
                    annotations=[KVPair(key="s", value="v")], reference_uuid=ref_uuid
                ),
                build(
                    annotations=[KVPair(key="int", value=42)], reference_uuid=ref_uuid
                ),
                build(
                    annotations=[KVPair(key="f", value=3.14)], reference_uuid=ref_uuid
                ),
                build(pass_number=None),
                build(pass_number=1),
                build(pass_number=2),
                build(averaged_of_n=3, reference_uuid=ref_uuid),
                build(
                    averaged_of_n=2,
                    reference_uuid=ref_uuid,
                    averaged_of_n_composed_of_n=2,
                ),
                build(noise=noise_uuid),  # Sample with noise trace
            ]
        )
        return built

    return build_with_potential_ref(with_ref=True) + build_with_potential_ref(
        with_ref=False
    )


def make_dummy_database(
    path: Path,
    device_serial_number: str | None = None,
    raster_id: uuid.UUID | None = None,
) -> tuple[RasterConfig, DeviceMetadata, RasterMetadata]:
    """Create a dummy database at the specified path for testing.

    This function creates a complete test database with diverse measurements
    that exercise various features of the grdb format.

    Args:
        path: Path where the .grf database file will be created.
        device_serial_number: Optional device serial number. Defaults to "123-ABC".
        raster_id: Optional raster ID. Defaults to None.

    Returns:
        Tuple containing the RasterConfig, DeviceMetadata, and RasterMetadata
        that were used to create the database.
    """
    config, device, meta = make_dummy_metadata(
        device_serial_number=device_serial_number,
        raster_id=raster_id,
    )
    measurements = make_measurement_variants()

    create_db(path, device, meta, config)
    add_pulses(path, measurements)
    return (config, device, meta)
