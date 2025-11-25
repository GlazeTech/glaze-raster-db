from __future__ import annotations

import json
import struct
import time
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, model_validator
from sqlalchemy import CheckConstraint, Column, MetaData, String, UniqueConstraint
from sqlmodel import Field, SQLModel

GRDB_METADATA = MetaData()

# Current schema version - increment when making breaking changes
CURRENT_SCHEMA_VERSION = 6
Axis = Literal["x", "y", "z"]
Sign = Literal[1, -1]

# String literal types for variants and composition types
TraceVariant = Literal["reference", "sample", "noise", "other"]
DatasetVariant = Literal["raster", "collection"]
PulseCompositionType = Literal["stitch", "average"]


class GRDBBase(SQLModel, metadata=GRDB_METADATA):
    """Base class for all GRDB models with an isolated metadata instance."""


class SchemaVersion(GRDBBase, table=True):
    """Tracks the schema version of the database."""

    __tablename__ = "schema_version"
    id: int = Field(primary_key=True)
    version: int = Field(default=CURRENT_SCHEMA_VERSION)


class DeviceMetadata(BaseModel):
    device_serial_number: str
    device_firmware_version: str


class Point3D(BaseModel):
    x: float | None
    y: float | None
    z: float | None


class Point3DFullyDefined(BaseModel):
    x: float
    y: float
    z: float


class RasterPattern(BaseModel):
    start_point: Point3D
    end_point: Point3D


class RasterConfig(BaseModel):
    patterns: list[RasterPattern]
    stepsize: float
    reference_point: Point3D | None
    acquire_ref_every: int | None
    repetitions_config: RepetitionsConfig | None = None


class KVPair(BaseModel):
    key: str
    value: str | int | float


class AxisMap(BaseModel):
    axis: Axis
    sign: Sign


class AxesMapping(BaseModel):
    x: AxisMap
    y: AxisMap
    z: AxisMap

    @model_validator(mode="after")
    def validate_unique_axis_mapping(self) -> AxesMapping:
        """Ensure each axis (x, y, z) maps to a unique target axis."""
        axes = [self.x.axis, self.y.axis, self.z.axis]
        if len(axes) != len(set(axes)):
            msg = "Each axis (x, y, z) must map to a unique target axis"
            raise ValueError(msg)
        return self


class CoordinateTransform(BaseModel):
    """Defines a coordinate system transformation between user and machine coordinates."""

    id: UUID  # uuid
    name: str  # user label
    offset: Point3DFullyDefined  # offset from user to machine coordinates
    mapping: AxesMapping
    last_used: int  # timestamp of last use (milliseconds since UNIX epoch)
    notes: str | None = None  # optional free text


class RepetitionsConfig(BaseModel):
    passes: int
    interval_millisecs: float  # in milliseconds

    @model_validator(mode="after")
    def validate_passes_config(self) -> RepetitionsConfig:
        """Ensure passes and interval are positive."""
        if self.passes <= 0:
            msg = "Passes must be positive"
            raise ValueError(msg)
        if self.interval_millisecs <= 0:
            msg = "Interval must be positive"
            raise ValueError(msg)
        return self


class RasterMetadata(BaseModel):
    variant: DatasetVariant
    app_version: str | None = None
    raster_id: UUID | None = None
    timestamp: int
    annotations: list[KVPair]
    device_configuration: dict[str, Any]
    user_coordinates: CoordinateTransform | None = None


class BaseTrace(BaseModel):
    time: list[float]
    signal: list[float]
    uuid: UUID
    timestamp: int
    noise: UUID | None


class PulseComposition(BaseModel):
    """User-facing composition item for a stitched pulse.

    Contains the source pulse data and metadata about how it was used.
    """

    pulse: BaseTrace
    position: int
    shift: float


class Trace(BaseTrace):
    """User-facing pulse model that may include stitching information.

    When a pulse is a stitched result, ``stitching_info`` lists the
    component pulses in their enumerated order with the applied shifts.
    """

    derived_from: list[PulseComposition] | None = None
    averaged_from: list["Trace"] | None = None  # noqa: UP037 - the recursive type breaks language servers like PyLance

    @model_validator(mode="after")
    def validate_lineage(self: Trace) -> Trace:
        """Ensure only one lineage style is present and averages have >= 2 sources."""
        if self.derived_from and self.averaged_from:
            msg = "Trace cannot have both derived_from and averaged_from"
            raise ValueError(msg)
        if self.averaged_from is not None and len(self.averaged_from) < 2:  # noqa: PLR2004
            msg = "averaged_from requires at least two source traces"
            raise ValueError(msg)
        if self.derived_from is not None and len(self.derived_from) < 2:  # noqa: PLR2004
            msg = "derived_from requires at least two source pulse"
            raise ValueError(msg)
        # Disallow nested averaging - averaged sources cannot be averaged
        if self.averaged_from:
            for source in self.averaged_from:
                if source.averaged_from is not None:
                    msg = "Nested averaging not allowed: averaged sources cannot themselves be averaged"
                    raise ValueError(msg)
        return self

    @classmethod
    def new(
        cls: type[Trace],
        times: list[float],
        signal: list[float],
        noise: UUID | None = None,
    ) -> Trace:
        """Create a new Trace with a unique UUID and empty data arrays."""
        return cls(
            time=times,
            signal=signal,
            uuid=uuid4(),
            timestamp=int(time.time() * 1000),
            noise=noise,
        )


class Measurement(BaseModel):
    pulse: Trace
    point: Point3D | None = None
    variant: TraceVariant
    reference: UUID | None = None
    annotations: list[KVPair] | None = None
    pass_number: int | None = None  # optional pass number for multi-pass rasters

    def get_annotation(self: Measurement, key: str) -> str | int | float | None:
        """Retrieve the value for a given annotation key, or None if not found."""
        if self.annotations is None:
            return None
        for kv in self.annotations:
            if kv.key == key:
                return kv.value
        return None


class RasterInfoDB(GRDBBase, table=True):
    """All metadata, configuration, and device info for a raster session."""

    __tablename__ = "raster_info"
    id: UUID = Field(default_factory=uuid4, primary_key=True)

    # DeviceMetadata fields
    device_serial_number: str
    device_firmware_version: str

    # RasterMetadata fields
    variant: DatasetVariant = Field(sa_column=Column(String))  # Added in v0.7.0
    app_version: str | None = None  # Made optional in v0.7.0
    timestamp: int
    annotations: str  # JSON string of list[KVPair]
    device_configuration: str  # JSON string

    # RasterConfig fields (made optional in v0.7.0 for collection variant)
    patterns: str | None = None  # JSON string
    stepsize: float | None = None
    reference_point: str | None = None  # JSON string
    acquire_ref_every: int | None = None

    # Repetitions configuration (added in v0.4.0)
    repetitions_config: str | None = None  # JSON string of RepetitionsConfig

    # Coordinate system transformation (added in v0.2.0)
    user_coordinates: str | None = None  # JSON string of CoordinateTransform

    @classmethod
    def from_api(
        cls: type[RasterInfoDB],
        meta: RasterMetadata,
        device: DeviceMetadata,
        config: RasterConfig | None = None,
    ) -> RasterInfoDB:
        # Validate variant-config consistency
        if meta.variant == "raster" and config is None:
            msg = "raster_config is required when variant='raster'"
            raise ValueError(msg)
        if meta.variant != "raster" and config is not None:
            msg = "raster_config should be None when variant is not 'raster'"
            raise ValueError(msg)

        return cls(
            id=meta.raster_id,
            device_serial_number=device.device_serial_number,
            device_firmware_version=device.device_firmware_version,
            variant=meta.variant,
            app_version=meta.app_version,
            timestamp=meta.timestamp,
            annotations=json.dumps([a.model_dump() for a in meta.annotations]),
            device_configuration=json.dumps(meta.device_configuration),
            patterns=(
                json.dumps([p.model_dump() for p in config.patterns])
                if config
                else None
            ),
            stepsize=config.stepsize if config else None,
            reference_point=(
                json.dumps(config.reference_point.model_dump())
                if config and config.reference_point
                else None
            ),
            acquire_ref_every=config.acquire_ref_every if config else None,
            user_coordinates=(
                json.dumps(meta.user_coordinates.model_dump(mode="json"))
                if meta.user_coordinates
                else None
            ),
            repetitions_config=(
                json.dumps(config.repetitions_config.model_dump())
                if config and config.repetitions_config
                else None
            ),
        )

    def to_raster_config(self: RasterInfoDB) -> RasterConfig | None:
        if self.patterns is None or self.stepsize is None:
            return None
        return RasterConfig(
            patterns=[
                RasterPattern.model_validate(p) for p in json.loads(self.patterns)
            ],
            stepsize=self.stepsize,
            reference_point=(
                Point3D.model_validate(json.loads(self.reference_point))
                if self.reference_point
                else None
            ),
            acquire_ref_every=self.acquire_ref_every,
            repetitions_config=(
                RepetitionsConfig.model_validate(json.loads(self.repetitions_config))
                if self.repetitions_config
                else None
            ),
        )

    def to_device_metadata(self: RasterInfoDB) -> DeviceMetadata:
        return DeviceMetadata(
            device_serial_number=self.device_serial_number,
            device_firmware_version=self.device_firmware_version,
        )

    def to_raster_metadata(self: RasterInfoDB) -> RasterMetadata:
        return RasterMetadata(
            variant=self.variant,
            raster_id=self.id,
            app_version=self.app_version,
            timestamp=self.timestamp,
            annotations=[
                KVPair.model_validate(a) for a in json.loads(self.annotations)
            ],
            device_configuration=json.loads(self.device_configuration),
            user_coordinates=self.to_coordinate_transform(),
        )

    def to_coordinate_transform(self: RasterInfoDB) -> CoordinateTransform | None:
        """Convert user_coordinates JSON string to CoordinateTransform object."""
        if self.user_coordinates is None:
            return None
        return CoordinateTransform.model_validate(json.loads(self.user_coordinates))


class PulseDB(GRDBBase, table=True):
    __tablename__ = "pulses"
    uuid: UUID = Field(primary_key=True)
    time: bytes  # packed float32 array
    signal: bytes
    timestamp: int  # ms since UNIX epoch
    x: float | None
    y: float | None
    z: float | None
    reference: UUID | None
    variant: TraceVariant = Field(sa_column=Column(String))
    noise: UUID | None
    pass_number: int | None
    annotations: str | None = Field(
        default_factory=lambda: json.dumps([])
    )  # JSON string of list[KVPair]

    @classmethod
    def from_measurement(cls: type[PulseDB], result: Measurement) -> PulseDB:
        time_bytes = cls.pack_floats(result.pulse.time)
        signal_bytes = cls.pack_floats(result.pulse.signal)
        return cls(
            time=time_bytes,
            signal=signal_bytes,
            timestamp=result.pulse.timestamp,
            uuid=result.pulse.uuid,
            x=result.point.x if result.point else None,
            y=result.point.y if result.point else None,
            z=result.point.z if result.point else None,
            reference=result.reference,
            variant=result.variant,
            pass_number=result.pass_number,
            annotations=json.dumps(
                [a.model_dump() for a in (result.annotations or [])]
            ),
            noise=result.pulse.noise,
        )

    @classmethod
    def from_basetrace(cls: type[PulseDB], measurement: BaseTrace) -> PulseDB:
        time_bytes = cls.pack_floats(measurement.time)
        signal_bytes = cls.pack_floats(measurement.signal)
        return PulseDB(
            time=time_bytes,
            signal=signal_bytes,
            timestamp=measurement.timestamp,
            uuid=measurement.uuid,
            noise=measurement.noise,
            x=None,
            y=None,
            z=None,
            reference=None,
            variant="other",
            annotations=json.dumps([]),
            pass_number=None,
        )

    def to_measurement(
        self: PulseDB,
        stitching: list[PulseComposition] | None,
        averaged_from: list[Trace] | None,
    ) -> Measurement:
        # Return None if all coordinates are None (collection-type data without spatial info)
        # Otherwise create Point3D (for backward compatibility with raster data)
        point = (
            None
            if self.x is None and self.y is None and self.z is None
            else Point3D(x=self.x, y=self.y, z=self.z)
        )
        return Measurement(
            pulse=Trace(
                uuid=self.uuid,
                timestamp=self.timestamp,
                noise=self.noise,
                time=self.unpack_floats(self.time),
                signal=self.unpack_floats(self.signal),
                derived_from=stitching,
                averaged_from=averaged_from,
            ),
            point=point,
            reference=self.reference,
            variant=self.variant,
            pass_number=self.pass_number,
            annotations=[
                KVPair.model_validate(a) for a in json.loads(self.annotations or "[]")
            ],
        )

    def to_basetrace(self: PulseDB) -> BaseTrace:
        return BaseTrace(
            uuid=self.uuid,
            timestamp=self.timestamp,
            noise=self.noise,
            time=self.unpack_floats(self.time),
            signal=self.unpack_floats(self.signal),
        )

    @staticmethod
    def pack_floats(values: list[float]) -> bytes:
        """Packs a list of floating-point numbers into a bytes object using little-endian format.

        Args:
            values: A list of floating-point numbers to be packed.

        Returns:
            bytes: A bytes object containing the packed floating-point numbers.
        """
        return struct.pack(f"<{len(values)}f", *values)

    @staticmethod
    def unpack_floats(blob: bytes) -> list[float]:
        """Unpacks a bytes object containing 32-bit floating-point numbers into a list of floats.

        Args:
            blob (bytes): A bytes object where each 4 bytes represent a 32-bit floating-point number in little-endian format.

        Returns:
            list[float]: A list of floats unpacked from the input bytes object.

        Raises:
            struct.error: If the length of the input bytes object is not a multiple of 4.
        """
        count = len(blob) // 4
        return list(struct.unpack(f"<{count}f", blob))


class PulseCompositionTable(GRDBBase, table=True):
    """Associates a derived pulse with its source pulses.

    A "final" (stitched or averaged) pulse is represented as a normal PulseDB. Its composition is captured here by
    linking the derived pulse UUID to one or more source pulse UUIDs.
    """

    __tablename__ = "pulse_composition"
    __table_args__ = (
        UniqueConstraint("final_uuid", "position"),
        UniqueConstraint("final_uuid", "source_uuid"),
        CheckConstraint(
            "("
            "composition_type = 'stitch' AND position IS NOT NULL AND shift IS NOT NULL"
            ") OR ("
            "composition_type = 'average' AND position IS NULL AND shift IS NULL"
            ")",
            name="ck_pulse_composition_type_fields",
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    final_uuid: UUID = Field(foreign_key="pulses.uuid", index=True)
    source_uuid: UUID = Field(foreign_key="pulses.uuid", index=True)
    position: int | None
    shift: float | None
    composition_type: PulseCompositionType = Field(sa_column=Column(String))
