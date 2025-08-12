import pytest

from grdb.models import AxesMapping, AxisMap


def test_valid_mapping() -> None:
    """Test that a valid mapping (no duplicates) works."""
    AxesMapping(
        x=AxisMap(axis="x", sign=1),
        y=AxisMap(axis="y", sign=1),
        z=AxisMap(axis="z", sign=1),
    )


def test_duplicate_mapping_errors() -> None:
    """Test that duplicate mappings raise ValueError."""
    with pytest.raises(ValueError, match=r"Each axis \(x, y, z\) must map to a unique"):
        AxesMapping(
            x=AxisMap(axis="x", sign=1),
            y=AxisMap(axis="x", sign=-1),  # duplicate: both x and y map to "x"
            z=AxisMap(axis="z", sign=1),
        )


def test_triplet_mapping_errors() -> None:
    """Test that triplet mappings raise ValueError."""
    with pytest.raises(ValueError, match=r"Each axis \(x, y, z\) must map to a unique"):
        AxesMapping(
            x=AxisMap(axis="x", sign=1),
            y=AxisMap(axis="x", sign=-1),
            z=AxisMap(axis="x", sign=1),
        )
