import shutil
import tempfile
from pathlib import Path

import pytest

from grdb.crud import (
    add_annotations,
    add_pulses,
    create_db,
    load_metadata,
    load_pulses,
)
from grdb.models import (
    BaseTrace,
    KVPair,
    Measurement,
    PulseComposition,
    PulseDB,
    TraceVariant,
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
    refs = make_dummy_measurement(variant=TraceVariant.reference)

    create_db(db_path, config, device, meta)
    # Add references after DB creation
    add_pulses(db_path, refs)

    (
        loaded_config,
        loaded_device,
        loaded_meta,
        n_ref,
        n_samp,
    ) = load_metadata(db_path)

    assert loaded_config == config
    assert loaded_device == device

    assert loaded_meta.annotations == meta.annotations
    assert loaded_meta.app_version == meta.app_version
    assert loaded_meta.timestamp == meta.timestamp
    assert loaded_meta.device_configuration == meta.device_configuration
    assert loaded_meta.user_coordinates == meta.user_coordinates

    assert n_ref == len(refs)
    assert n_samp == 0


def test_append_and_batch(db_path: Path) -> None:
    config, device, meta = make_dummy_metadata()
    refs = make_dummy_measurement(
        variant=TraceVariant.reference
    ) + make_dummy_measurement(composed_of_n=1, variant=TraceVariant.reference)
    sams = make_dummy_measurement(variant=TraceVariant.sample) + make_dummy_measurement(
        composed_of_n=2, variant=TraceVariant.sample
    )
    create_db(db_path, config, device, meta)

    # Add references and samples after DB creation
    add_pulses(db_path, sams + refs)
    refs_loaded = load_pulses(
        db_path, offset=0, limit=1000, variant=TraceVariant.reference
    )
    samples_loaded = load_pulses(
        db_path, offset=0, limit=1000, variant=TraceVariant.sample
    )

    _assert_raster_results_are_equal(refs, refs_loaded)
    _assert_raster_results_are_equal(sams, samples_loaded)


def test_update_annotations_and_reload(db_path: Path) -> None:
    config, device, meta = make_dummy_metadata()
    create_db(db_path, config, device, meta)
    # Update annotations
    new_annotations = [KVPair(key="x", value=1), KVPair(key="y", value=2)]
    add_annotations(db_path, new_annotations)
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

    create_db(db_path, config, device, meta)
    add_pulses(db_path, variants)

    loaded = load_pulses(db_path, offset=0, limit=1_000_000)

    _assert_raster_results_are_equal(loaded, variants)


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
                temp_path, offset=0, limit=1000, variant=TraceVariant.reference
            )
            sams_existing = load_pulses(
                temp_path, offset=0, limit=1000, variant=TraceVariant.sample
            )
            variants = make_measurement_variants()
            ref_variants = [m for m in variants if m.variant == TraceVariant.reference]
            sam_variants = [m for m in variants if m.variant == TraceVariant.sample]
            add_pulses(temp_path, variants)
            # Load references and samples using the new API filter
            refs_loaded = load_pulses(
                temp_path, offset=0, limit=1_000_000, variant=TraceVariant.reference
            )
            sams_loaded = load_pulses(
                temp_path, offset=0, limit=1_000_000, variant=TraceVariant.sample
            )
            _assert_raster_results_are_equal(refs_existing + ref_variants, refs_loaded)
            _assert_raster_results_are_equal(sams_existing + sam_variants, sams_loaded)


def test_create_db_and_unlink_file(db_path: Path) -> None:
    """Test creating a database file, writing to it, and then unlinking it."""
    # Create database with some data
    config, device, meta = make_dummy_metadata()
    # Create and save the database (no pulses yet)
    create_db(db_path, config, device, meta)

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


def _assert_raster_results_are_equal(
    results1: list[Measurement], results2: list[Measurement]
) -> None:
    """Assert that two lists of RasterResult are equal in content."""
    assert len(results1) == len(results2)
    results2_by_uuid = {res.pulse.uuid: res for res in results2}
    for res1 in results1:
        res2 = results2_by_uuid.get(res1.pulse.uuid)
        assert res2 is not None
        assert res1.pulse.timestamp == res2.pulse.timestamp
        assert res1.pulse.time == pytest.approx(res2.pulse.time)
        assert res1.pulse.signal == pytest.approx(res2.pulse.signal)
        assert res1.point == res2.point
        assert res1.reference == res2.reference
        assert res1.variant == res2.variant

        _assert_annotations_are_equal(res1, res2)
        _assert_derived_from_are_equal(res1, res2)


def _assert_derived_from_are_equal(res1: Measurement, res2: Measurement) -> None:
    """Assert that the derived_from compositions of two Measurement instances are equal."""
    if res1.pulse.derived_from is None:
        assert res2.pulse.derived_from is None
    else:
        assert res2.pulse.derived_from is not None
        assert len(res1.pulse.derived_from) == len(res2.pulse.derived_from)
        for comp1, comp2 in zip(res1.pulse.derived_from, res2.pulse.derived_from):
            _assert_pulse_compositions_are_equal(comp1, comp2)


def _assert_pulse_compositions_are_equal(
    comp1: PulseComposition, comp2: PulseComposition
) -> None:
    """Assert that two PulseComposition instances are equal in content."""
    assert comp1.position == comp2.position
    assert comp1.shift == pytest.approx(comp2.shift)
    _assert_base_measurements_are_equal(comp1.pulse, comp2.pulse)


def _assert_base_measurements_are_equal(meas1: BaseTrace, meas2: BaseTrace) -> None:
    """Assert that two BaseMeasurement instances are equal in content."""
    assert meas1.uuid == meas2.uuid
    assert meas1.timestamp == meas2.timestamp
    assert meas1.time == pytest.approx(meas2.time)
    assert meas1.signal == pytest.approx(meas2.signal)


def _assert_annotations_are_equal(res1: Measurement, res2: Measurement) -> None:
    """Assert that the annotations of two Measurement instances are equal."""
    if res1.annotations is None or len(res1.annotations) == 0:
        assert res2.annotations is None or len(res2.annotations) == 0
    else:
        assert res2.annotations is not None
        assert len(res1.annotations) == len(res2.annotations)
        for ann1, ann2 in zip(res1.annotations, res2.annotations):
            assert ann1.key == ann2.key
            assert ann1.value == ann2.value
