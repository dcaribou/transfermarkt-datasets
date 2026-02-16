# Phase 0 Exit Report

## Summary
Phase 0 set out to prove the multi-source canonical vision with a working end-to-end pipeline integrating a non-Transfermarkt source. This report evaluates outcomes against the Phase 0 success criteria defined in the [MVP contract](phase0-mvp-contract.md).

## Success Criteria Assessment

### 1. Functional Completeness
**Status: ACHIEVED**

OpenFootball is integrated end-to-end for all three in-scope assets:
- **Acquire**: `scripts/acquiring/openfootball.py` downloads match data from the openfootball/football.json GitHub repo.
- **Stage**: `stg_openfootball_competitions`, `stg_openfootball_clubs`, `stg_openfootball_games` parse raw JSON into normalized tables.
- **Curate**: Curated `competitions`, `clubs`, and `games` models union both sources via canonical mappings.
- **Publish**: CI pipeline builds, tests, validates provenance, and commits prepared data.

### 2. Contract Correctness
**Status: ACHIEVED**

- Row-level provenance columns (`source_system`, `source_record_id`, `ingested_at`) are present and populated in all in-scope curated outputs.
- Field-level provenance mapping artifact exists at `dbt/field_provenance.yml` with documented derivation rules for all fields in competitions, clubs, and games.
- Provenance spec documented in `docs/field-level-provenance.md`.

### 3. Quality Gate Viability
**Status: ACHIEVED**

- dbt tests cover PK uniqueness, not-null constraints, accepted values, and row count ranges for all staging and curated models.
- Provenance validation step in CI checks column presence and null-free `source_system`.
- Dynamic profiling script (`scripts/profiling/profile_models.py`) computes baselines and detects drift for row counts, null ratios, and cardinality.
- CI build pipeline gates on all three check layers.

### 4. Backward Compatibility
**Status: ACHIEVED**

- All existing curated columns are preserved unchanged.
- New provenance columns are additive (appended to schema).
- Curated test column sets include the new columns without removing any existing ones.
- Row count thresholds widened to accommodate OpenFootball additions, with upper bounds set conservatively.

### 5. Operability
**Status: ACHIEVED**

- `just acquire_openfootball` runs reproducibly on any machine with Python and network access.
- GitHub Actions workflow `acquire-openfootball.yml` scheduled weekly with manual dispatch.
- DVC tracking (`data/raw/openfootball.dvc`) enables versioned raw data management.
- Full build from raw to curated runs without manual intervention.

## Deliverables Produced

| Deliverable | Location |
|---|---|
| Source-agnostic path convention | `transfermarkt_datasets/core/utils.py:raw_data_path()` |
| OpenFootball acquirer | `scripts/acquiring/openfootball.py` |
| DVC + CI for OpenFootball | `data/raw/openfootball.dvc`, `.github/workflows/acquire-openfootball.yml` |
| dbt staging models | `dbt/models/base/openfootball/` (3 models + sources + tests) |
| Canonical mapping tables | `dbt/models/canonical/` (competitions, clubs, matches) |
| Multi-source curated models | `dbt/models/curated/competitions.sql`, `clubs.sql`, `games.sql` |
| Field-level provenance | `docs/field-level-provenance.md`, `dbt/field_provenance.yml` |
| Profiling and drift checks | `scripts/profiling/profile_models.py` |
| CI quality gates | `.github/workflows/build.yml` (provenance + profiling steps) |
| Updated docs | `README.md` (architecture, lineage, multi-source) |

## Known Gaps and Follow-ups for Phase 1

### Entity Resolution Coverage
- Club name matching is basic (exact + substring). More sophisticated fuzzy matching or a curated mapping table would improve coverage.
- Match ID mapping depends on having both canonical club IDs resolved. Unresolved clubs cascade to unresolved matches.

### OpenFootball Data Scope
- OpenFootball provides no player-level data (by design, this is a Phase 0 non-goal).
- Some fields remain Transfermarkt-only (stadium, attendance, referee, formations, manager names). OpenFootball rows have these as NULL.
- OpenFootball coverage is limited to leagues/seasons available in the football.json repo.

### Profiling Baselines
- Initial baselines need to be set from a full production build. The profiling script is ready but baselines.json is not yet populated.

### Schema Versioning
- No formal schema versioning policy is in place yet. Phase 1 should establish this per the vision doc.

## Go/No-Go Recommendation

**Recommendation: GO**

All five success criteria are met. The architecture is proven end-to-end with OpenFootball as the first non-Transfermarkt source. The canonical mapping, provenance tracking, and quality gating infrastructure is in place and extensible. The project is ready to proceed to Phase 1 (Foundation and Narrative) and Phase 2 (Source Abstraction) with confidence that the multi-source approach works.

### Priorities for Phase 1
1. Define canonical entity contracts (fields, keys, constraints).
2. Establish schema versioning policy and governance docs.
3. Improve club entity resolution (fuzzy matching, curated overrides).
4. Set production profiling baselines.
5. Draft source onboarding checklist for future sources.
