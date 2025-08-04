import time
import uuid

from grdb.models import (
    CoordinateTransform,
    DeviceMetadata,
    KVPair,
    Measurement,
    Point3D,
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
        working_coordinates=make_dummy_coordinate_transform(),
    )
    return (config, device, meta)


def make_dummy_coordinate_transform() -> CoordinateTransform:
    """Create a dummy coordinate transform for testing."""
    return CoordinateTransform(
        translation=Point3D(x=10.0, y=20.0, z=30.0),
        rotation=Point3D(x=0.1, y=0.2, z=0.3),
        scale=Point3D(x=1.5, y=1.5, z=1.5),
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
