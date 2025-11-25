# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`grdb` is a Python library for creating and managing raster SQLite databases of pulse measurement data from GlazeTech devices. The library provides a schema-versioned SQLite format (.grf files) for storing time-domain pulse waveforms with associated spatial coordinates and metadata.

## Development Commands

### Testing
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=grdb --cov-report=term-missing

# Run a specific test file
pytest tests/test_crud.py

# Run a specific test function
pytest tests/test_crud.py::test_function_name
```

### Linting and Type Checking
```bash
# Run ruff linter (fix issues automatically)
ruff check --fix .

# Run mypy type checker
mypy grdb

# Format code with ruff
ruff format .
```

### Version Management
```bash
# Bump version (uses bumpver)
# This updates version in pyproject.toml and grdb/__init__.py
bumpver update --patch  # 0.6.0 -> 0.6.1
bumpver update --minor  # 0.6.0 -> 0.7.0
bumpver update --major  # 0.6.0 -> 1.0.0
```

### Installation
```bash
# Install in editable mode with dev dependencies
pip install -e ".[dev]"
```

## Architecture

### Core Components

**Models (`grdb/models.py`)**: Defines the data models and database schema
- **Pydantic models** (user-facing API): `Measurement`, `Trace`, `BaseTrace`, `Point3D`, `RasterConfig`, `RasterMetadata`, `DeviceMetadata`
- **SQLModel tables** (database layer): `PulseDB`, `RasterInfoDB`, `PulseCompositionTable`, `SchemaVersion`
- Pulse waveforms are stored as packed float32 binary blobs (`time` and `signal` fields)
- Schema versioning via `CURRENT_SCHEMA_VERSION` constant

**CRUD Operations (`grdb/crud.py`)**: Database interaction layer
- `create_db()`: Creates new .grf file with metadata
- `add_pulses()`: Appends measurements to existing database
- `load_pulses()`: Retrieves measurements with automatic stitching/averaging info reconstruction
- `load_metadata()`: Extracts configuration and metadata
- Uses SQLModel/SQLAlchemy with NullPool for thread safety

**Migrations (`grdb/migrations.py`)**: Schema evolution
- `MIGRATION_SCRIPTS` dict maps schema versions to migration functions
- Migrations run automatically when opening older databases
- Each migration updates `SchemaVersion` table

**Core (`grdb/core.py`)**: Table creation utilities
- `create_tables()`: Creates all required SQLite tables

### Key Concepts

**Pulse Composition**: Pulses can be derived from other pulses through:
- **Stitching** (`derived_from`): Multiple source pulses combined with position/shift metadata
- **Averaging** (`averaged_from`): Multiple source pulses averaged together
- Source pulses are stored in `PulseDB` but excluded from user-facing queries
- The `PulseCompositionTable` tracks the relationships between final and source pulses

**Trace Variants**: Four types defined in `TraceVariant` enum:
- `reference`: Reference measurements at known locations
- `sample`: Sample measurements on the object being scanned
- `noise`: Baseline noise measurements
- `other`: Other measurement types

**Final vs Source Pulses**:
- "Final" pulses are user-facing measurements
- "Source" pulses are components used to build final pulses via stitching/averaging
- Queries filter out source pulses using: `~PulseDB.uuid.in_(select(PulseCompositionTable.source_uuid))`

**Multi-pass Rasters**: Supported via `pass_number` field and `repetitions_config` in `RasterConfig`

**Coordinate Transforms**: Optional `user_coordinates` in `RasterMetadata` maps between user and machine coordinate systems

## Database Schema

The SQLite database contains 4 main tables:

1. **pulses**: Stores pulse waveforms (time/signal as binary), spatial coordinates (x/y/z), variant, reference UUID, annotations, pass_number, noise UUID
2. **raster_info**: Single-row table with all metadata (device info, raster config, app version, timestamps, annotations)
3. **pulse_composition**: Links final pulses to their source pulses for stitching/averaging
4. **schema_version**: Tracks current schema version for migrations

## Type Safety

This codebase uses strict mypy configuration:
- `disallow_any_unimported = true`
- `disallow_untyped_defs = true`
- `check_untyped_defs = true`
- All functions must have type hints
- Use `from __future__ import annotations` for forward references

## Code Style

Ruff is configured with `select = ["ALL"]` but specific rules are ignored (see `pyproject.toml`).
Tests have relaxed rules (no docstring requirements, magic numbers allowed, asserts permitted).
Docstrings follow Google style convention.
