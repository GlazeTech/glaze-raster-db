import random
import time
import uuid

from grdb.models import (
    AxesMapping,
    AxisMap,
    BaseMeasurement,
    CoordinateTransform,
    DeviceMetadata,
    KVPair,
    Measurement,
    Point3D,
    Point3DFullyDefined,
    PulseComposition,
    PulseVariant,
    RasterConfig,
    RasterMetadata,
    RasterPattern,
    RasterResult,
)


def make_dummy_metadata() -> tuple[RasterConfig, DeviceMetadata, RasterMetadata]:
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
    )
    device = DeviceMetadata(
        device_serial_number="123-ABC",
        device_firmware_version="v1.0.0",
    )
    meta = RasterMetadata(
        app_version="app1",
        timestamp=161803398,
        annotations=[KVPair(key="foo", value="bar"), KVPair(key="baz", value=1.0)],
        device_configuration={"mode": "test"},
        user_coordinates=make_dummy_coordinate_transform(),
    )
    return (config, device, meta)


def make_dummy_coordinate_transform() -> CoordinateTransform:
    """Create a dummy coordinate transform for testing."""
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


def make_dummy_raster_results(
    variant: PulseVariant,
    n_results: int = 2,
    pulse_length: int = 3,
    composed_of_n: int = 0,
) -> list[RasterResult]:
    return [
        RasterResult(
            pulse=make_dummy_measurement(
                pulse_length=pulse_length, composed_of_n=composed_of_n
            ),
            point=Point3D(x=float(i), y=float(i), z=float(i)),
            reference=None,
            variant=variant,
        )
        for i in range(n_results)
    ]


def make_dummy_measurement(
    pulse_length: int = 2, composed_of_n: int = 0
) -> Measurement:
    composition = (
        make_dummy_composition(composed_of_n=composed_of_n, pulse_length=pulse_length)
        if composed_of_n > 0
        else None
    )
    return Measurement(
        time=[1.0 * i for i in range(pulse_length)],
        signal=[random.random() for _ in range(pulse_length)],  # noqa: S311
        uuid=uuid.uuid4(),
        timestamp=int(time.time() * 1000),  # Time in ms since Unix epoch
        derived_from=composition,
    )


def make_dummy_composition(
    composed_of_n: int = 2, pulse_length: int = 2
) -> list[PulseComposition]:
    return [
        PulseComposition(
            pulse=make_dummy_base_measurement(pulse_length=pulse_length),
            position=i,
            shift=i * 10e-12,
        )
        for i in range(composed_of_n)
    ]


def make_dummy_base_measurement(pulse_length: int = 2) -> BaseMeasurement:
    return BaseMeasurement(
        time=[1.0 * i for i in range(pulse_length)],
        signal=[random.random() for _ in range(pulse_length)],  # noqa: S311
        uuid=uuid.uuid4(),
        timestamp=int(time.time() * 1000),  # Time in ms since Unix epoch
    )
