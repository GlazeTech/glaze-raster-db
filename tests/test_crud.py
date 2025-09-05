import shutil
import tempfile
from pathlib import Path

import pytest
from sqlalchemy.exc import IntegrityError

from grdb.crud import (
    add_pulse_compositions_to_db,
    append_pulses_to_db,
    create_and_save_raster_db,
    load_pulse_batch_from_db,
    load_raster_metadata_from_db,
    update_raster_annotations,
)
from grdb.mock import (
    make_dummy_composed_raster_result,
    make_dummy_metadata,
    make_dummy_raster_results,
)
from grdb.models import KVPair, PulseDB


def test_pack_unpack_floats_roundtrip() -> None:
    data = [0.0, -1.5, 3.14, 42.0]
    packed = PulseDB.pack_floats(data)
    unpacked = PulseDB.unpack_floats(packed)
    assert unpacked == pytest.approx(data)


def test_crud_create_and_load(db_path: Path) -> None:
    config, device, meta = make_dummy_metadata()
    refs = make_dummy_raster_results()

    create_and_save_raster_db(db_path, config, device, meta, refs)

    (
        loaded_config,
        loaded_device,
        loaded_meta,
        n_ref,
        n_samp,
    ) = load_raster_metadata_from_db(db_path)

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
    refs = make_dummy_raster_results()
    sams = make_dummy_raster_results()

    create_and_save_raster_db(db_path, config, device, meta, refs)
    # Append sample pulses
    append_pulses_to_db(db_path, sams)
    # Load batch
    refs_loaded, samples_loaded = load_pulse_batch_from_db(
        db_path, offset=0, limit=len(refs) * 2
    )
    assert len(refs_loaded) == len(refs)  # one reference
    assert len(samples_loaded) == len(sams)  # one appended sample


def test_update_annotations_and_reload(db_path: Path) -> None:
    config, device, meta = make_dummy_metadata()
    refs = make_dummy_raster_results()

    create_and_save_raster_db(db_path, config, device, meta, refs)
    # Update annotations
    new_annotations = [KVPair(key="x", value=1), KVPair(key="y", value=2)]
    update_raster_annotations(db_path, new_annotations)
    # Reload metadata
    _, _, reloaded_meta, _, _ = load_raster_metadata_from_db(db_path)
    keys_values = {pair.key: pair.value for pair in reloaded_meta.annotations}
    assert keys_values == {"x": 1, "y": 2}


def test_load_metadata_no_file(db_path: Path) -> None:
    non_existent = db_path / "nofile.db"
    with pytest.raises(FileNotFoundError):
        load_raster_metadata_from_db(non_existent)


def test_backward_load_compatibility() -> None:
    """Test that v0.1.0 databases can be loaded correctly."""
    # Copy the test asset to our test location
    for p in (Path(__file__).parent / "test_assets").iterdir():
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir) / p.name
            shutil.copy2(p, temp_path)
            # First load runs migrations
            load_raster_metadata_from_db(temp_path)
            # Load twice to ensure it works after migration scripts have run
            load_raster_metadata_from_db(temp_path)


def test_load_pulse_batch_excludes_composed_pulses(db_path: Path) -> None:
    """Test that load_pulse_batch_from_db only loads 'final' pulses, not those used as compositions."""
    config, device, meta = make_dummy_metadata()
    refs = make_dummy_raster_results()

    create_and_save_raster_db(db_path, config, device, meta, refs)

    sams = make_dummy_raster_results()
    # Create a final "stitched" pulse from component parts
    final_pulse, pulse_parts, compositions = make_dummy_composed_raster_result(
        n_composed=3
    )
    append_pulses_to_db(db_path, [*pulse_parts, *sams, final_pulse])
    add_pulse_compositions_to_db(db_path, compositions)

    # Load pulses using the batch function
    refs_loaded, samples_loaded = load_pulse_batch_from_db(db_path, offset=0, limit=100)

    # Check that the UUIDs of loaded pulses match the expected ones
    expected_ref_uuids = [r.pulse.uuid for r in refs]
    expected_sample_uuids = [s.pulse.uuid for s in sams] + [final_pulse.pulse.uuid]

    loaded_ref_uuids = [r.pulse.uuid for r in refs_loaded]
    loaded_sample_uuids = [s.pulse.uuid for s in samples_loaded]

    assert set(loaded_ref_uuids) == set(expected_ref_uuids)
    assert set(loaded_sample_uuids) == set(expected_sample_uuids)


def test_create_db_and_unlink_file(db_path: Path) -> None:
    """Test creating a database file, writing to it, and then unlinking it."""
    # Create database with some data
    config, device, meta = make_dummy_metadata()
    refs = make_dummy_raster_results()

    # Create and save the database
    create_and_save_raster_db(db_path, config, device, meta, refs)

    # Verify the file was created and has content
    assert db_path.exists()
    assert db_path.stat().st_size > 0

    # Now try to unlink (delete) the file
    db_path.unlink()

    # Verify the file is gone
    assert not db_path.exists()

    # Verify that trying to read from the deleted file raises FileNotFoundError
    with pytest.raises(FileNotFoundError):
        load_raster_metadata_from_db(db_path)


def test_pulse_composition_unique_derived_position_constraint(db_path: Path) -> None:
    """Test that the unique constraint on (derived_uuid, position) is enforced."""
    config, device, meta = make_dummy_metadata()
    refs = make_dummy_raster_results()

    create_and_save_raster_db(db_path, config, device, meta, refs)

    # Create test pulses
    pulse_parts = make_dummy_raster_results(n_results=2)
    final_pulse = make_dummy_raster_results(n_results=1)[0]
    final_pulse, pulse_parts, compositions = make_dummy_composed_raster_result(
        n_composed=2
    )
    # Add the pulses to the database
    append_pulses_to_db(db_path, [*pulse_parts, final_pulse])

    # Create two compositions with the same derived_uuid and position
    for i in range(len(compositions)):
        compositions[i].position = 0  # Force same position

    # This should raise an IntegrityError due to the unique constraint
    with pytest.raises(IntegrityError):
        add_pulse_compositions_to_db(db_path, compositions)
