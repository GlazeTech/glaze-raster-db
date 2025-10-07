# grdb

A Python library for creating and managing raster SQLite databases of pulse measurement data.


## Installation

To install the latest version
```bash
pip install git+https://github.com/GlazeTech/glaze-raster-db.git
```

To install a specific version (e.g. v0.1.0)
```bash
pip install git+https://github.com/GlazeTech/glaze-raster-db.git@0.1.0
```


## Examples

### Loading Pulses

**Load sample pulses:**
```python
from pathlib import Path
from grdb import load_pulses

db_path = Path("raster.grf")

# Load first 100 sample pulses
samples = load_pulses(db_path, offset=0, limit=100, variant="sample")
```

**Load reference pulses:**
```python
from pathlib import Path
from grdb import load_pulses

db_path = Path("raster.grf")

# Load first 50 reference pulses
references = load_pulses(db_path, offset=0, limit=50, variant="reference")
```

**Load all pulses (both samples and references):**
```python
from pathlib import Path
from grdb import load_pulses

db_path = Path("raster.grf")

# Load all pulse types (variant=None)
all_pulses = load_pulses(db_path, offset=0, limit=1000, variant=None)
```

### Understanding the Measurement Data Structure

The `load_pulses` function returns a list of `Measurement` objects. Each `Measurement` represents a single pulse measurement with its associated metadata and spatial information.

#### Measurement Fields

A `Measurement` object contains the following fields:

- **`pulse`** (`Trace`): The actual pulse data containing:
  - `time` (list[float]): Time values for the pulse waveform
  - `signal` (list[float]): Signal amplitude values corresponding to each time point
  - `uuid` (UUID): Unique identifier for this pulse
  - `timestamp` (int): Unix timestamp in milliseconds when the pulse was acquired
  - `derived_from` (list[PulseComposition] | None): If this pulse was created by stitching multiple pulses together, this contains information about the source pulses

- **`point`** (`Point3D`): The 3D spatial coordinates where this pulse was measured:
  - `x` (float | None): X coordinate
  - `y` (float | None): Y coordinate  
  - `z` (float | None): Z coordinate

- **`variant`** (`TraceVariant`): The type of measurement, one of:
  - `"reference"`: Reference measurement taken at a known location
  - `"sample"`: Sample measurement taken on the object being scanned
  - `"noise"`: Noise measurement for baseline correction
  - `"other"`: Other types of measurements

- **`reference`** (UUID | None): If this is a sample measurement, this field may contain the UUID of the associated reference pulse

- **`annotations`** (list[KVPair] | None): Optional key-value pairs for additional metadata

#### Example: Accessing Measurement Data

```python
from pathlib import Path
from grdb import load_pulses

db_path = Path("raster.grf")

# Load sample pulses
samples = load_pulses(db_path, offset=0, limit=10, variant="sample")

# Access the first measurement
first_measurement = samples[0]

# Access pulse waveform data
time_values = first_measurement.pulse.time
signal_values = first_measurement.pulse.signal

# Access spatial coordinates
x_position = first_measurement.point.x
y_position = first_measurement.point.y
z_position = first_measurement.point.z

# Check the measurement type
measurement_type = first_measurement.variant

# Get the pulse UUID
pulse_id = first_measurement.pulse.uuid
```
