# Design

## Context

See proposal.md - Why.
In `gcsfs/core.py`, `GCSFileSystem._merge(path, paths, acl=None)` sends a `POST` request to `b/{bucket}/o/{key}/compose`. Currently, it maps source paths via `[{"name": self.split_path(p)[1]} for p in paths]`, discarding `split_path(p)[2]` (the generation ID). This causes GCS to resolve each source name to its latest live generation instead of the pinned historical generation. Furthermore, `generation` from the destination `path` is currently ignored instead of being rejected, inconsistent with `_cp_file` and `_pipe_file`.

## Goals / Non-Goals

**Goals:**
- Retain source generation IDs when present on input paths and pass them to GCS `compose` in `sourceObjects`.
- Raise `ValueError("Cannot write to specific object generation")` when destination path has an explicit generation.
- Retain full backward compatibility for unversioned source paths (`generation is None`).

**Non-Goals:**
- Enabling server-side compose for Zonal buckets (which remains unsupported by GCS architecture and raises `NotImplementedError` in `ExtendedGcsFileSystem._merge`).

## Decisions

### 1. Structure of `sourceObjects` in `composeRequest`
- **Decision**: For each path in `paths`, unpack `(_, src_key, src_generation) = self.split_path(p)`. If `src_generation` is non-empty/truthy, set `entry["generation"] = src_generation`.
- **Rationale**: The GCS JSON API documentation for `objects.compose` accepts `sourceObjects[].generation` as an optional integer/string. Omitting it when `src_generation is None` preserves existing behavior of composing live objects.
- **Alternatives Considered**: Passing `generation=None` explicitly in the dictionary. Rejected because the GCS REST API expects the field to be omitted or a valid generation integer/string.

### 2. Destination Validation
- **Decision**: Check `if generation: raise ValueError("Cannot write to specific object generation")` on the destination path immediately after `self.split_path(path)`.
- **Rationale**: Aligns directly with existing guards in `_cp_file` and `_pipe_file`.

## Risks / Trade-offs

- **[Risk] Path parsing with `#` in file names**:
  - **Mitigation**: `split_path` only parses `#` as generation when `version_aware=True` and the fragment is digits. When `version_aware=False`, `#` remains part of the object name, preserving backward compatibility.
