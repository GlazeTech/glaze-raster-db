from __future__ import annotations

import random
import time
import uuid

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
    Trace,
    TraceVariant,
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


def make_dummy_measurement(
    variant: TraceVariant,
    n_results: int = 2,
    pulse_length: int = 3,
    composed_of_n: int = 0,
) -> list[Measurement]:
    return [
        Measurement(
            pulse=make_dummy_trace(
                pulse_length=pulse_length, composed_of_n=composed_of_n
            ),
            point=Point3D(x=float(i), y=float(i), z=float(i)),
            reference=None,
            variant=variant,
            annotations=[],
        )
        for i in range(n_results)
    ]


def make_dummy_trace(pulse_length: int = 2, composed_of_n: int = 0) -> Trace:
    composition = (
        make_dummy_composition(composed_of_n=composed_of_n, pulse_length=pulse_length)
        if composed_of_n > 0
        else None
    )
    return Trace(
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
            pulse=make_dummy_basetrace(pulse_length=pulse_length),
            position=i,
            shift=i * 10e-12,
        )
        for i in range(composed_of_n)
    ]


def make_dummy_basetrace(pulse_length: int = 2) -> BaseTrace:
    return BaseTrace(
        time=[1.0 * i for i in range(pulse_length)],
        signal=[random.random() for _ in range(pulse_length)],  # noqa: S311
        uuid=uuid.uuid4(),
        timestamp=int(time.time() * 1000),  # Time in ms since Unix epoch
    )


def make_measurement_variants() -> list[Measurement]:
    """Generate a comprehensive set of Measurements covering key branches.

    Branches covered:
    - Point3D: fully defined; partially defined (one None); all None.
    - Annotations: mixed types; empty KV; empty list.
    - Variants: reference, sample, noise, other.
    - Reference field: set vs unset (sample referencing a ref).
    - Stitching: present (derived_from) vs absent.
    """

    def build(
        *,
        point: Point3D | None = None,
        variant: TraceVariant | None = TraceVariant.sample,
        annotations: list[KVPair] | None = None,
        composed_of_n: int | None = 0,
        reference_uuid: uuid.UUID | None = None,
        pass_number: int | None = None,
    ) -> Measurement:
        point = point or Point3D(x=None, y=None, z=None)
        variant = variant or TraceVariant.sample
        composed_of_n = composed_of_n or 0

        return Measurement(
            pulse=make_dummy_trace(composed_of_n=composed_of_n),
            point=point,
            reference=reference_uuid,
            variant=variant,
            annotations=annotations,
            pass_number=pass_number,
        )

    def build_with_potential_ref(*, with_ref: bool = False) -> list[Measurement]:
        built = []
        if with_ref:
            ref = build(variant=TraceVariant.reference)
            ref_uuid = ref.pulse.uuid
            built.append(ref)
        else:
            ref_uuid = None
        built.extend(
            [
                build(composed_of_n=2, reference_uuid=ref_uuid),
                build(point=Point3D(x=1.0, y=2.0, z=3.0), reference_uuid=ref_uuid),
                build(point=Point3D(x=4.0, y=None, z=6.0), reference_uuid=ref_uuid),
                build(variant=TraceVariant.reference, reference_uuid=ref_uuid),
                build(variant=TraceVariant.sample, reference_uuid=ref_uuid),
                build(variant=TraceVariant.noise, reference_uuid=ref_uuid),
                build(variant=TraceVariant.other, reference_uuid=ref_uuid),
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
            ]
        )
        return built

    return build_with_potential_ref(with_ref=True) + build_with_potential_ref(
        with_ref=False
    )
