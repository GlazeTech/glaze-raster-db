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
from grdb.crud import load_pulses
from grdb.models import TraceVariant

db_path = Path("raster.grf")

# Load first 100 sample pulses
samples = load_pulses(db_path, offset=0, limit=100, variant=TraceVariant.sample)
```

**Load reference pulses:**
```python
from pathlib import Path
from grdb.crud import load_pulses
from grdb.models import TraceVariant

db_path = Path("raster.grf")

# Load first 50 reference pulses
references = load_pulses(db_path, offset=0, limit=50, variant=TraceVariant.reference)
```

**Load all pulses (both samples and references):**
```python
from pathlib import Path
from grdb.crud import load_pulses

db_path = Path("raster.grf")

# Load all pulse types (variant=None)
all_pulses = load_pulses(db_path, offset=0, limit=1000, variant=None)
```
