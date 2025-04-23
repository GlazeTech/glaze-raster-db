# grdb

A Python library for creating and managing raster SQLite databases of pulse measurement data.


## Installation

```bash
pip install grdb @ git+ssh://git@github.com/GlazeTech/glaze-raster-db.git@<VERSION_NUMBER>",
```

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

# Load a batch of pulses
refs_batch, samples_batch = load_pulse_batch_from_db(db_path, offset=0, limit=50)

# Update annotations
new_annotations = [KVPair(key="status", value="verified")]
update_raster_annotations(db_path, new_annotations)
```

