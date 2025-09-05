import time
import uuid

from grdb.models import (
    AxesMapping,
    AxisMap,
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
    n_results: int = 2, pulse_length: int = 3
) -> list[RasterResult]:
    return [
        RasterResult(
            pulse=Measurement(
                time=[i * 0.1 for i in range(pulse_length)],
                signal=[float(i) for i in range(pulse_length)],
                uuid=uuid.uuid4(),
                timestamp=int(time.time() * 1000),  # Time in ms since Unix epoch
            ),
            point=Point3D(x=float(i), y=float(i), z=float(i)),
            reference=None,
        )
        for i in range(n_results)
    ]


def make_dummy_composed_raster_result(
    n_composed: int,
) -> tuple[RasterResult, list[RasterResult], list[PulseComposition]]:
    """Create a list of dummy RasterResults and a PulseComposition linking them.

    Args:
        n_composed: Number of component pulses to compose into a final pulse.
    """
    pulse_parts = make_dummy_raster_results(n_results=n_composed, pulse_length=2)
    final_pulse = make_dummy_raster_results(n_results=1, pulse_length=4)[0]

    compositions = [
        PulseComposition(
            derived_uuid=final_pulse.pulse.uuid,
            source_uuid=part.pulse.uuid,
            position=i,
            shift=i * 10e-12,
        )
        for i, part in enumerate(pulse_parts)
    ]
    return final_pulse, pulse_parts, compositions
