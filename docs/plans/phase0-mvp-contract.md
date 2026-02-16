# Phase 0 MVP Contract: First Non-Transfermarkt Source

## Epic Metadata
- Type: `epic`
- Suggested labels: `phase-0`, `architecture`, `product`
- Status: `draft` -> `approved` (set by maintainer when accepted)
- Scope horizon: Phase 0 only (first non-Transfermarkt source)

## Purpose
Define the minimum canonical asset contract for onboarding the first non-Transfermarkt source into the existing pipeline without breaking current curated consumers.

## In-Scope Canonical Assets (Exact)
The first non-Transfermarkt source is in-scope only for these canonical asset families:

1. `competitions`
2. `clubs`
3. `games` (matches)

### Required scope detail per asset
- `competitions`: competition identity and season-compatible competition records required for canonical joins.
- `clubs`: club identity records required to support competition participation and game endpoints.
- `games`: game identity, participants (home/away club linkage), and scheduling/outcome fields needed by current curated game-level consumers.

### Minimum delivery shape
- Source-specific acquisition and staging implemented for this source.
- Curated models ingest this source for the three assets above.
- Existing curated schema remains stable for current consumers (no unversioned breaking changes).

## Explicit Non-Goals (Phase 0)
The following are explicitly out of scope for this source in Phase 0:

1. Player valuations from this source.
2. Player-level canonical onboarding (players/appearances/events/transfers) from this source.
3. Advanced event semantics beyond the current canonical `games` contract.
4. Historical backfill beyond the agreed pilot window for Phase 0.
5. Replacing Transfermarkt as primary source for any existing downstream output.

## Required Lineage and Provenance Outputs
For all curated rows produced from the first non-Transfermarkt source, contract requires lineage/provenance fields and behavior:

1. Row-level provenance columns present in curated outputs touched by this source:
- `source_system`
- `source_record_id`
- `ingested_at`

2. Field-level provenance requirement for derived/coalesced fields:
- A reproducible mapping artifact exists that identifies contributing source field(s) for each derived canonical field.
- Mapping must be versioned in-repo and referenced by transformation logic.

3. Traceability requirement:
- From any curated row for in-scope assets, maintainers can trace back to the original staged/raw source record deterministically.

4. Multi-source behavior requirement:
- When canonical rows can be formed from multiple sources, precedence/coalescing rules must be explicit and documented alongside the model.

## Success Metrics (Acceptance Metrics)
Phase 0 is successful when all criteria below are true:

1. Functional completeness
- Non-Transfermarkt source is live end-to-end for `competitions`, `clubs`, and `games` (acquire -> stage -> curate -> publish).

2. Contract correctness
- Required provenance columns are present and populated for in-scope curated rows.
- Field-level provenance mapping artifact exists for all derived fields in in-scope assets.

3. Quality gate viability
- Automated checks run in CI/local for in-scope assets and detect schema/volume/null regressions relevant to this source.

4. Backward compatibility
- Existing curated outputs consumed by current users remain stable (or changes are versioned and documented with migration notes).

5. Operability
- Pipeline runs reproducibly for the Phase 0 pilot window without manual one-off intervention.

## Child Issue Contract (Required)
Every child issue under this epic must:

1. Include a direct reference to `docs/plans/phase0-mvp-contract.md`.
2. State which in-scope asset(s) it affects: `competitions`, `clubs`, and/or `games`.
3. State whether it changes lineage/provenance behavior and how.
4. Declare if any non-goal boundary is being challenged (must be explicit and approved before implementation).

Suggested issue line:
`Contract: docs/plans/phase0-mvp-contract.md`

## Dependencies
- None.
