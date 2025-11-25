import shutil
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, delete
from sqlalchemy.pool import NullPool
from sqlmodel import Session

from grdb import (
    add_pulses,
    create_db,
    load_metadata,
    load_pulses,
    update_annotations,
)
from grdb.core import create_tables
from grdb.models import (
    BaseTrace,
    DeviceMetadata,
    KVPair,
    Measurement,
    PulseComposition,
    PulseDB,
    RasterInfoDB,
    RasterMetadata,
    SchemaVersion,
    Trace,
)
from tests.mock import (
    make_dummy_measurement,
    make_dummy_metadata,
    make_measurement_variants,
)


def test_pack_unpack_floats_roundtrip() -> None:
    data = [0.0, -1.5, 3.14, 42.0]
    packed = PulseDB.pack_floats(data)
    unpacked = PulseDB.unpack_floats(packed)
    assert unpacked == pytest.approx(data)


def test_crud_create_and_load(db_path: Path) -> None:
    config, device, meta = make_dummy_metadata()
    refs = make_dummy_measurement(variant="reference")

    create_db(db_path, device, meta, config)
    # Add references after DB creation
    add_pulses(db_path, refs)

    (
        loaded_config,
        loaded_device,
        loaded_meta,
        n_ref,
        n_samp,
    ) = load_metadata(db_path)
    meta.raster_id = loaded_meta.raster_id  # IDs are generated on creation

    assert loaded_config == config
    assert loaded_device == device
    assert loaded_meta == meta
    assert n_ref == len(refs)
    assert n_samp == 0


def test_append_and_batch(db_path: Path) -> None:
    config, device, meta = make_dummy_metadata()
    refs = make_dummy_measurement(variant="reference") + make_dummy_measurement(
        composed_of_n=2, variant="reference"
    )
    sams = make_dummy_measurement(variant="sample") + make_dummy_measurement(
        composed_of_n=2, variant="sample"
    )
    create_db(db_path, device, meta, config)

    # Add references and samples after DB creation
    add_pulses(db_path, sams + refs)
    refs_loaded = load_pulses(db_path, offset=0, limit=1000, variant="reference")
    samples_loaded = load_pulses(db_path, offset=0, limit=1000, variant="sample")

    _assert_measurements_are_equal(refs, refs_loaded)
    _assert_measurements_are_equal(sams, samples_loaded)


def test_update_annotations_and_reload(db_path: Path) -> None:
    config, device, meta = make_dummy_metadata()
    create_db(db_path, device, meta, config)
    # Update annotations
    new_annotations = [KVPair(key="x", value=1), KVPair(key="y", value=2)]
    update_annotations(db_path, new_annotations)
    # Reload metadata
    _, _, reloaded_meta, _, _ = load_metadata(db_path)
    keys_values = {pair.key: pair.value for pair in reloaded_meta.annotations}
    assert keys_values == {"x": 1, "y": 2}


def test_measurement_field_variations_roundtrip(db_path: Path) -> None:
    """Ensure Measurements round-trip with varied points and annotations.

    Uses mock generator to hit key branches of Measurement construction.
    """
    config, device, meta = make_dummy_metadata()
    variants = make_measurement_variants()

    create_db(db_path, device, meta, config)
    add_pulses(db_path, variants)

    loaded = load_pulses(db_path, offset=0, limit=1_000_000)

    _assert_measurements_are_equal(loaded, variants)


def test_load_metadata_no_file(db_path: Path) -> None:
    non_existent = db_path / "nofile.db"
    with pytest.raises(FileNotFoundError):
        load_metadata(non_existent)


def test_backward_load_compatibility() -> None:
    """Test that v0.1.0 databases can be loaded correctly."""
    # Copy the test asset to our test location
    for p in (Path(__file__).parent / "test_assets").iterdir():
        if p.suffix != ".grdb":
            continue
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir) / p.name
            shutil.copy2(p, temp_path)
            # First load runs migrations
            load_metadata(temp_path)
            # Load twice to ensure it works after migration scripts have run
            load_metadata(temp_path)

            # Assert we can add and load pulses
            refs_existing = load_pulses(
                temp_path, offset=0, limit=1000, variant="reference"
            )
            sams_existing = load_pulses(
                temp_path, offset=0, limit=1000, variant="sample"
            )
            variants = make_measurement_variants()
            ref_variants = [m for m in variants if m.variant == "reference"]
            sam_variants = [m for m in variants if m.variant == "sample"]
            add_pulses(temp_path, variants)
            # Load references and samples using the new API filter
            refs_loaded = load_pulses(
                temp_path, offset=0, limit=1_000_000, variant="reference"
            )
            sams_loaded = load_pulses(
                temp_path, offset=0, limit=1_000_000, variant="sample"
            )
            _assert_measurements_are_equal(refs_existing + ref_variants, refs_loaded)
            _assert_measurements_are_equal(sams_existing + sam_variants, sams_loaded)


def test_create_db_and_unlink_file(db_path: Path) -> None:
    """Test creating a database file, writing to it, and then unlinking it."""
    # Create database with some data
    config, device, meta = make_dummy_metadata()
    # Create and save the database (no pulses yet)
    create_db(db_path, device, meta, config)

    # Verify the file was created and has content
    assert db_path.exists()
    assert db_path.stat().st_size > 0

    # Now try to unlink (delete) the file
    db_path.unlink()

    # Verify the file is gone
    assert not db_path.exists()

    # Verify that trying to read from the deleted file raises FileNotFoundError
    with pytest.raises(FileNotFoundError):
        load_metadata(db_path)


def test_load_metadata_empty_db(db_path: Path) -> None:
    """Test that load_metadata raises ValueError on a database with no metadata."""
    # Create a proper database with schema
    engine = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
    create_tables(engine)

    # Add schema version but delete all metadata rows
    with Session(engine) as session:
        session.add(SchemaVersion())
        session.commit()
        # Delete all metadata rows
        session.exec(delete(RasterInfoDB))
        session.commit()

    # Try to load metadata from database with no metadata
    with pytest.raises(ValueError, match="No metadata found in DB"):
        load_metadata(db_path)


def test_update_annotations_empty_db(db_path: Path) -> None:
    """Test that update_annotations raises ValueError on a database with no metadata."""
    # Create a proper database with schema
    engine = create_engine(f"sqlite:///{db_path}", poolclass=NullPool)
    create_tables(engine)

    # Add schema version but delete all metadata rows
    with Session(engine) as session:
        session.add(SchemaVersion())
        session.commit()
        # Delete all metadata rows
        session.exec(delete(RasterInfoDB))
        session.commit()

    # Try to update annotations on database with no metadata
    annotations = [KVPair(key="test", value=123)]
    with pytest.raises(ValueError, match="No metadata found in DB"):
        update_annotations(db_path, annotations)


def test_create_collection_db(db_path: Path) -> None:
    """Test creating a collection-type database without raster_config."""

    device = DeviceMetadata(
        device_serial_number="TEST-123",
        device_firmware_version="v2.0.0",
    )
    meta = RasterMetadata(
        variant="collection",
        app_version="test_app",
        timestamp=1234567890,
        annotations=[KVPair(key="dataset", value="test_collection")],
        device_configuration={"mode": "collection"},
    )

    # Create collection database without raster_config
    create_db(db_path, device, meta)

    # Load metadata and verify
    loaded_config, loaded_device, loaded_meta, n_ref, n_samp = load_metadata(db_path)

    assert loaded_config is None  # No raster config for collection variant
    assert loaded_device == device
    assert loaded_meta.variant == "collection"
    assert loaded_meta.app_version == meta.app_version
    assert n_ref == 0
    assert n_samp == 0


def test_collection_db_with_none_points(db_path: Path) -> None:
    """Test adding measurements with None points to collection database."""

    device = DeviceMetadata(
        device_serial_number="TEST-123",
        device_firmware_version="v2.0.0",
    )
    meta = RasterMetadata(
        variant="collection",
        timestamp=1234567890,
        annotations=[],
        device_configuration={},
    )

    create_db(db_path, device, meta)

    # Create measurements with None points
    measurements = [
        Measurement(
            pulse=make_dummy_measurement("sample")[0].pulse,
            point=None,  # No spatial coordinates
            variant="sample",
        )
        for _ in range(3)
    ]

    add_pulses(db_path, measurements)
    loaded = load_pulses(db_path, offset=0, limit=100)
    _assert_measurements_are_equal(measurements, loaded)


def test_raster_variant_requires_config(db_path: Path) -> None:
    """Test that raster variant requires raster_config."""

    device = DeviceMetadata(
        device_serial_number="TEST-123",
        device_firmware_version="v2.0.0",
    )
    meta = RasterMetadata(
        variant="raster",  # Raster variant
        timestamp=1234567890,
        annotations=[],
        device_configuration={},
    )

    # Should raise ValueError because raster variant requires raster_config
    with pytest.raises(
        ValueError, match="raster_config is required when variant='raster'"
    ):
        create_db(db_path, device, meta)  # No raster_config provided


def test_collection_variant_rejects_config(db_path: Path) -> None:
    """Test that collection variant cannot have raster_config."""

    config, device, _ = make_dummy_metadata()

    meta = RasterMetadata(
        variant="collection",  # Collection variant
        timestamp=1234567890,
        annotations=[],
        device_configuration={},
    )

    # Should raise ValueError because collection variant cannot have raster_config
    with pytest.raises(
        ValueError, match="raster_config should be None when variant is not 'raster'"
    ):
        create_db(db_path, device, meta, config)  # Config should not be provided


def _assert_measurements_are_equal(
    results1: list[Measurement], results2: list[Measurement]
) -> None:
    """Assert that two lists of Measurements are equal in content."""
    assert len(results1) == len(results2)
    results2_by_uuid = {res.pulse.uuid: res for res in results2}
    for res1 in results1:
        res2 = results2_by_uuid.get(res1.pulse.uuid)
        assert res2 is not None
        assert res1.point == res2.point
        assert res1.reference == res2.reference
        assert res1.variant == res2.variant
        assert res1.pass_number == res2.pass_number
        _assert_traces_are_equal(res1.pulse, res2.pulse)
        _assert_annotations_are_equal(res1, res2)


def _assert_derived_from_are_equal(res1: Trace, res2: Trace) -> None:
    """Assert that the derived_from compositions of two Trace instances are equal."""
    if res1.derived_from is None:
        assert res2.derived_from is None
    else:
        assert res2.derived_from is not None
        assert len(res1.derived_from) == len(res2.derived_from)
        for comp1, comp2 in zip(res1.derived_from, res2.derived_from, strict=True):
            _assert_pulse_compositions_are_equal(comp1, comp2)


def _assert_averaged_from_are_equal(res1: Trace, res2: Trace) -> None:
    """Assert that the averaged_from sources of two Trace instances are equal."""
    if res1.averaged_from is None:
        assert res2.averaged_from is None
    else:
        assert res2.averaged_from is not None
        assert len(res1.averaged_from) == len(res2.averaged_from)
        for trace1, trace2 in zip(res1.averaged_from, res2.averaged_from, strict=True):
            _assert_traces_are_equal(trace1, trace2)


def _assert_pulse_compositions_are_equal(
    comp1: PulseComposition, comp2: PulseComposition
) -> None:
    """Assert that two PulseComposition instances are equal in content."""
    assert comp1.position == comp2.position
    assert comp1.shift == pytest.approx(comp2.shift)
    _assert_base_traces_are_equal(comp1.pulse, comp2.pulse)


def _assert_base_traces_are_equal(meas1: BaseTrace, meas2: BaseTrace) -> None:
    """Assert that two BaseTrace instances are equal in content."""
    assert meas1.uuid == meas2.uuid
    assert meas1.timestamp == meas2.timestamp
    assert meas1.time == pytest.approx(meas2.time)
    assert meas1.signal == pytest.approx(meas2.signal)
    assert meas1.noise == meas2.noise


def _assert_annotations_are_equal(res1: Measurement, res2: Measurement) -> None:
    """Assert that the annotations of two Measurement instances are equal."""
    if res1.annotations is None or len(res1.annotations) == 0:
        assert res2.annotations is None or len(res2.annotations) == 0
    else:
        assert res2.annotations is not None
        assert len(res1.annotations) == len(res2.annotations)
        for ann1, ann2 in zip(res1.annotations, res2.annotations, strict=False):
            assert ann1.key == ann2.key
            assert ann1.value == ann2.value


def _assert_traces_are_equal(trace1: Trace, trace2: Trace) -> None:
    """Assert that two Trace instances are equal in content, including compositions."""
    _assert_base_traces_are_equal(trace1, trace2)
    _assert_derived_from_are_equal(trace1, trace2)
    _assert_averaged_from_are_equal(trace1, trace2)
