__version__ = "0.6.0"

# Re-export public API functions for convenient imports, e.g.:
from .crud import (
    add_pulses,
    create_db,
    load_metadata,
    load_pulses,
    update_annotations,
)

__all__ = [
    "__version__",
    "add_pulses",
    "create_db",
    "load_metadata",
    "load_pulses",
    "update_annotations",
]
