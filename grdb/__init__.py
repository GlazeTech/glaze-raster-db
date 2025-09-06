__version__ = "0.3.1"

# Re-export public API functions for convenient imports, e.g.:
from .crud import (
    add_annotations,
    add_pulses,
    create_and_save_raster_db,
    load_metadata,
    load_pulses,
)

__all__ = [
    "__version__",
    "add_annotations",
    "add_pulses",
    "create_and_save_raster_db",
    "load_metadata",
    "load_pulses",
    "read_pulse_compositions_from_db",
]
