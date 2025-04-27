import json
import struct
from typing import Any, Optional, Union
from uuid import UUID, uuid4

from pydantic import BaseModel
from sqlalchemy import MetaData
from sqlmodel import Field, SQLModel

GRDB_METADATA = MetaData()


class GRDBBase(SQLModel, metadata=GRDB_METADATA):
    """Base class for all GRDB models with an isolated metadata instance."""


class DeviceMetadata(BaseModel):
    device_serial_number: str
    device_firmware_version: str


class Point3D(BaseModel):
    x: Optional[float]
    y: Optional[float]
    z: Optional[float]


class RasterPattern(BaseModel):
    start_point: Point3D
    end_point: Point3D


class RasterConfig(BaseModel):
    patterns: list[RasterPattern]
    stepsize: float
    reference_point: Optional[Point3D]
    acquire_ref_every: Optional[int]


class KVPair(BaseModel):
    key: str
    value: Union[str, int, float]


class RasterMetadata(BaseModel):
    app_version: str
    timestamp: int
    annotations: list[KVPair]
    device_configuration: dict[str, Any]


class Measurement(BaseModel):
    time: list[float]
    signal: list[float]
    uuid: UUID
    timestamp: int


class RasterResult(BaseModel):
    pulse: Measurement
    point: Point3D
    reference: Optional[UUID] = None


class RasterInfoDB(GRDBBase, table=True):
    """All metadata, configuration, and device info for a raster session."""

    __tablename__ = "raster_info"
    id: UUID = Field(default_factory=uuid4, primary_key=True)

    # DeviceMetadata fields
    device_serial_number: str
    device_firmware_version: str

    # RasterMetadata fields
    app_version: str
    timestamp: int
    annotations: str  # JSON string of list[KVPair]
    device_configuration: str  # JSON string

    # RasterConfig fields
    patterns: str  # JSON string
    stepsize: float
    reference_point: Optional[str] = None  # JSON string
    acquire_ref_every: Optional[int] = None

    @classmethod
    def from_api(
        cls: type["RasterInfoDB"],
        config: "RasterConfig",
        meta: "RasterMetadata",
        device: "DeviceMetadata",
    ) -> "RasterInfoDB":
        return cls(
            device_serial_number=device.device_serial_number,
            device_firmware_version=device.device_firmware_version,
            app_version=meta.app_version,
            timestamp=meta.timestamp,
            annotations=json.dumps([a.model_dump() for a in meta.annotations]),
            device_configuration=json.dumps(meta.device_configuration),
            patterns=json.dumps([p.model_dump() for p in config.patterns]),
            stepsize=config.stepsize,
            reference_point=(
                json.dumps(config.reference_point.model_dump())
                if config.reference_point
                else None
            ),
            acquire_ref_every=config.acquire_ref_every,
        )

    def to_raster_config(self: "RasterInfoDB") -> RasterConfig:
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
        )

    def to_device_metadata(self: "RasterInfoDB") -> DeviceMetadata:
        return DeviceMetadata(
            device_serial_number=self.device_serial_number,
            device_firmware_version=self.device_firmware_version,
        )

    def to_raster_metadata(self: "RasterInfoDB") -> RasterMetadata:
        return RasterMetadata(
            app_version=self.app_version,
            timestamp=self.timestamp,
            annotations=[
                KVPair.model_validate(a) for a in json.loads(self.annotations)
            ],
            device_configuration=json.loads(self.device_configuration),
        )


class PulseDB(GRDBBase, table=True):
    __tablename__ = "pulses"
    uuid: UUID = Field(primary_key=True)
    time: bytes  # packed float32 array
    signal: bytes
    timestamp: int  # ms since UNIX epoch
    is_reference: bool
    x: Optional[float]
    y: Optional[float]
    z: Optional[float]
    reference: Optional[UUID]

    @classmethod
    def from_raster_result(
        cls: type["PulseDB"],
        result: "RasterResult",
        *,
        is_reference: bool,
    ) -> "PulseDB":
        time_bytes = cls.pack_floats(result.pulse.time)
        signal_bytes = cls.pack_floats(result.pulse.signal)
        return cls(
            is_reference=is_reference,
            time=time_bytes,
            signal=signal_bytes,
            timestamp=result.pulse.timestamp,
            uuid=result.pulse.uuid,
            x=result.point.x,
            y=result.point.y,
            z=result.point.z,
            reference=result.reference,
        )

    def to_raster_result(self: "PulseDB") -> RasterResult:
        return RasterResult(
            pulse=Measurement(
                uuid=self.uuid,
                timestamp=self.timestamp,
                time=self.unpack_floats(self.time),
                signal=self.unpack_floats(self.signal),
            ),
            point=Point3D(x=self.x, y=self.y, z=self.z),
            reference=self.reference,
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
