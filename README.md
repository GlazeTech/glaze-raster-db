# grdb

A Python library for creating and managing raster SQLite databases of pulse measurement data.


## Installation

```bash
pip install grdb @ git+ssh://git@github.com/GlazeTech/glaze-raster-db.git@<VERSION_NUMBER>",
```


## Pulse Composition

Some pulses are stitched from one or more source pulses. In the public API this is represented by `RasterResult.pulse.derived_from`, which is a list of `PulseComposition` items containing the source `BaseMeasurement`, its `position` in the stitched order, and the applied `shift`.

- Persistence: When inserting pulses via `create_and_save_raster_db` (references) or `add_pulses` (samples), the library:
  - Stores each final (stitched or non-stitched) pulse in the `pulses` table.
  - If `derived_from` is present, ensures each source pulse exists in `pulses` with minimal fields (time, signal, uuid, timestamp) and `x/y/z/reference=None`.
  - Writes one row per component to `pulse_composition` linking `final_uuid -> source_uuid` with the recorded `position` and `shift`.
  - Uniqueness constraints on `(final_uuid, position)` and `(final_uuid, source_uuid)` prevent duplicates.

- Loading: `load_pulses` returns only user-facing “final” pulses — i.e., pulses that are not used as a source in any composition. For stitched pulses, it reconstructs `derived_from` by joining `pulse_composition` with the stored source pulses and ordering by `position`.

- Counting: `load_metadata` reports counts of reference and sample pulses among these final pulses only. Internal source components are excluded from the totals.

## Quickstart

```python
from pathlib import Path
from grdb.crud import (
    create_and_save_raster_db,
    append_pulses_to_db,
    load_raster_metadata_from_db,
    load_pulse_batch_from_db,
    update_raster_annotations,
)
from grdb.models import (
    RasterConfig,
    DeviceMetadata,
    RasterMetadata,
    CoordinateTransform,
    Point3D,
    KVPair,
    RasterResult,
)

# Define paths and metadata/config
db_path = Path("raster.db")
raster_config = RasterConfig(
    patterns=[...],  # list of RasterPattern
    stepsize=0.5,
    reference_point=None,
    acquire_ref_every=10,
)

device_meta = DeviceMetadata(
    device_serial_number="SN123456",
    device_firmware_version="1.0.0",
)

annotations = [KVPair(key="operator", value="Alice")]
raster_meta = RasterMetadata(
    app_version="0.1.0",
    timestamp=1616161616161,
    annotations=annotations,
    device_configuration={"mode": "auto"},
)

# Reference pulses
references: list[RasterResult] = [...]  # gather RasterResult objects

# Optional: Define coordinate system transformation in metadata
user_coords = CoordinateTransform(
    translation=Point3D(x=1000.0, y=2000.0, z=0.0),  # Offset in machine units
    rotation=Point3D(x=0.0, y=0.0, z=0.785398),     # 45° rotation around Z
    scale=Point3D(x=1000.0, y=1000.0, z=1000.0),    # μm to mm conversion
)
raster_meta.user_coordinates = user_coords  # Add to metadata

# Create and populate the database
create_and_save_raster_db(
    db_path,
    raster_config,
    device_meta,
    raster_meta,
    references,
)

# Append additional sample pulses
sample_pulses: list[RasterResult] = [...]  # new RasterResult objects
append_pulses_to_db(db_path, sample_pulses)

# Load metadata and counts
config, dev_meta, meta, n_refs, n_samples = load_raster_metadata_from_db(db_path)
print(f"Loaded {n_refs} reference and {n_samples} sample pulses")
if meta.user_coordinates:
    print(f"Database has coordinate transform with scale: {meta.user_coordinates.scale}")
# You can also access coordinates from the metadata
if meta.user_coordinates:
    print(f"Metadata contains coordinate transform: {meta.user_coordinates}")

# Load a batch of pulses
refs_batch, samples_batch = load_pulse_batch_from_db(db_path, offset=0, limit=50)

# Update annotations
new_annotations = [KVPair(key="status", value="verified")]
update_raster_annotations(db_path, new_annotations)
```
