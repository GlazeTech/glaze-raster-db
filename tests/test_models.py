from uuid import uuid4

import pytest

from grdb.models import (
    AxesMapping,
    AxisMap,
    PulseComposition,
    RepetitionsConfig,
    Trace,
)


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


def test_repetitions_config_passes_positive() -> None:
    """Test that RepetitionsConfig rejects non-positive passes."""
    with pytest.raises(ValueError, match="Passes must be positive"):
        RepetitionsConfig(passes=0, interval_millisecs=100.0)

    with pytest.raises(ValueError, match="Passes must be positive"):
        RepetitionsConfig(passes=-1, interval_millisecs=100.0)


def test_repetitions_config_interval_positive() -> None:
    """Test that RepetitionsConfig rejects non-positive interval."""
    with pytest.raises(ValueError, match="Interval must be positive"):
        RepetitionsConfig(passes=2, interval_millisecs=0.0)

    with pytest.raises(ValueError, match="Interval must be positive"):
        RepetitionsConfig(passes=2, interval_millisecs=-10.0)


def test_trace_both_derived_and_averaged() -> None:
    """Test that Trace cannot have both derived_from and averaged_from."""
    simple_trace = Trace(
        time=[1.0, 2.0],
        signal=[0.5, 0.6],
        uuid=uuid4(),
        timestamp=1000,
        noise=None,
    )

    comp = PulseComposition(pulse=simple_trace, position=0, shift=0.0)

    with pytest.raises(ValueError, match="cannot have both derived_from and averaged_from"):
        Trace(
            time=[1.0, 2.0],
            signal=[0.5, 0.6],
            uuid=uuid4(),
            timestamp=1000,
            noise=None,
            derived_from=[comp, comp],
            averaged_from=[simple_trace, simple_trace],
        )


def test_trace_averaged_from_min_sources() -> None:
    """Test that averaged_from requires at least 2 sources."""
    simple_trace = Trace(
        time=[1.0, 2.0],
        signal=[0.5, 0.6],
        uuid=uuid4(),
        timestamp=1000,
        noise=None,
    )

    with pytest.raises(ValueError, match="averaged_from requires at least two source traces"):
        Trace(
            time=[1.0, 2.0],
            signal=[0.5, 0.6],
            uuid=uuid4(),
            timestamp=1000,
            noise=None,
            averaged_from=[simple_trace],
        )


def test_trace_derived_from_min_sources() -> None:
    """Test that derived_from requires at least 2 sources."""
    simple_trace = Trace(
        time=[1.0, 2.0],
        signal=[0.5, 0.6],
        uuid=uuid4(),
        timestamp=1000,
        noise=None,
    )

    comp = PulseComposition(pulse=simple_trace, position=0, shift=0.0)

    with pytest.raises(ValueError, match="derived_from requires at least two source pulse"):
        Trace(
            time=[1.0, 2.0],
            signal=[0.5, 0.6],
            uuid=uuid4(),
            timestamp=1000,
            noise=None,
            derived_from=[comp],
        )


def test_trace_no_nested_averaging() -> None:
    """Test that nested averaging is not allowed."""
    simple_trace = Trace(
        time=[1.0, 2.0],
        signal=[0.5, 0.6],
        uuid=uuid4(),
        timestamp=1000,
        noise=None,
    )

    # Create an averaged trace
    averaged_trace = Trace(
        time=[1.0, 2.0],
        signal=[0.5, 0.6],
        uuid=uuid4(),
        timestamp=1000,
        noise=None,
        averaged_from=[simple_trace, simple_trace],
    )

    # Try to create a trace averaged from averaged traces (should fail)
    with pytest.raises(ValueError, match="Nested averaging not allowed"):
        Trace(
            time=[1.0, 2.0],
            signal=[0.5, 0.6],
            uuid=uuid4(),
            timestamp=1000,
            noise=None,
            averaged_from=[averaged_trace, averaged_trace],
        )
