# Spec Delta

## Purpose

Defines requirements for composing and concatenating version-pinned objects into a destination object within GCS buckets without losing version constraints.

## ADDED Requirements

### Requirement: Source object generation preservation
When composing objects, the filesystem SHALL preserve any explicit object generation specified on source paths and include it in each corresponding entry of the compose API request body.

#### Scenario: Composing with version-pinned source objects
- **WHEN** `merge` is invoked with source paths that include object generation specifiers
- **THEN** each corresponding `sourceObjects` item in the `compose` API payload includes `"name"` and `"generation"`

### Requirement: Destination generation rejection
When composing objects, the filesystem SHALL reject destination paths that specify an object generation, ensuring callers cannot target a specific historical version for overwrite.

#### Scenario: Merge rejected for version-pinned destination
- **WHEN** `merge` is invoked with a destination path that specifies an object generation
- **THEN** a `ValueError` stating `"Cannot write to specific object generation"` is raised before making any API calls
