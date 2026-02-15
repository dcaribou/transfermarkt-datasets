# Vision: From Transfermarkt Dataset to Open Professional Football Data Standard

## Vision Description

### North Star
Build the most trusted open dataset for professional football analysis by evolving from a Transfermarkt-first pipeline into a multi-source, canonical data platform.

### What Success Looks Like
1. The project is source-agnostic: Transfermarkt is one source among several.
2. Analysts can query a single coherent schema across competitions, countries, and seasons.
3. Data quality and freshness are transparent and measurable.
4. The project is the default open reference used by analysts, creators, and researchers.

### Strategic Positioning
The long-term differentiation is not "having one more scraper," but being the canonical standardization and quality layer across open professional football sources. Canonical modeling is the core product.

## Risk Analysis

### 1. Competence Risk (External Alternatives)
#### Risk
Other open projects already cover parts of this space (source connectors, historical coverage, event data, or language-specific ecosystems). If this project does not differentiate, growth may plateau.

#### Why It Matters
Users will adopt the project that gives them the highest trust-to-effort ratio:
- broad coverage
- stable schema
- reproducible quality
- active maintenance

#### Mitigation
1. Focus positioning on canonical modeling, not just extraction.
2. Expose source lineage and confidence as part of the dataset metadata at row/entity level, and at field level where fields may be derived from multiple sources.
3. Set a public quality bar (contracts, tests, freshness SLAs, change logs).
4. Prioritize interoperability (clear IDs, stable schemas, versioning).

#### Target Outcome
Become the project that people use to *reconcile* and *standardize* open football data, not only to download raw snapshots.

### 2. Scale Risk (Coverage and Architecture)
#### Risk
Expanding leagues, countries, and sources can expose bottlenecks in ingestion, model coupling, ID consistency, and test assumptions.

As source count grows, data quality assessment itself becomes a major challenge. Manual validation does not scale and data drift can go unnoticed.

#### Current Constraints to Address
1. Transfermarkt-specific naming and paths across ingestion and workflows.
2. Source-specific dbt source definitions tied to concrete folders.
3. Static row-count quality thresholds likely to break as scope grows.
4. Entity linking across sources is not yet a first-class layer.

#### Mitigation
1. Introduce source-agnostic ingestion conventions (`data/raw/<source>/<season>/...`).
2. Split dbt into source-specific staging + source-agnostic curated models.
3. Replace static volume assertions with automated dynamic profiling and anomaly checks that trigger on deviation from expected distributions/ranges.
4. Add canonical ID mapping tables for players, clubs, competitions, and matches.

#### Target Outcome
Scale by adding sources and competitions with linear operational effort and controlled schema impact.

### 3. Brand Risk (Repositioning Without Losing Trust)
#### Risk
A full rebrand can create perceived discontinuity if users think the original dataset is being abandoned.

#### Why It Matters
Trust is path-dependent: existing users value continuity, predictable updates, and familiar references.

#### Mitigation
1. Do a staged rebrand: change narrative first, repository name later.
2. Keep "formerly transfermarkt-datasets" visible during transition.
3. Publish migration notes and timeline before any rename.
4. Preserve links and downstream compatibility during the transition window.

#### Target Outcome
Retain existing audience trust while expanding project identity beyond a single source.

## Path to the Vision

### Phase 0: MVP Repositioning Proof (0-8 weeks)
#### Objectives
1. Prove the multi-source canonical vision with a working end-to-end pipeline.
2. Demonstrate one additional non-Transfermarkt source integrated into the same curated model layer.

#### Deliverables
1. Refactored architecture required for multi-source ingestion and staging.
2. One additional source onboarded end-to-end: acquire -> stage -> curate -> publish.
3. Canonical mappings for key entities touched by the new source.
4. Initial automated profiling/check framework enabled for the new source.

#### Exit Criteria
- One non-Transfermarkt source is live in curated outputs.
- Curated outputs preserve schema stability for current consumers.
- Automated quality checks catch meaningful deviations without manual inspection.

### Phase 1: Foundation and Narrative (parallel to Phase 0, 0-8 weeks)
#### Objectives
1. Clarify positioning publicly: from source-specific dataset to open canonical platform.
2. Prepare architecture for source expansion without breaking consumers.

#### Deliverables
1. Updated README vision statement and roadmap.
2. Source lineage/provenance model in curated outputs (`source_system`, `source_record_id`, `ingested_at`) and field-level provenance metadata for derived fields.
3. Draft canonical entity contracts for players, clubs, competitions, and matches.
4. Initial governance docs: versioning policy and schema change policy.

#### Exit Criteria
- Vision and scope are explicit in public docs.
- Existing users can still consume current outputs with no breaking changes.

### Phase 2: Source Abstraction (6-12 weeks)
#### Objectives
1. Decouple transformations from any single source implementation.
2. Make adding a new source a repeatable engineering process.

#### Deliverables
1. Standard raw data folder convention and acquirer interface.
2. dbt staging layer per source (`stg_<source>_*`).
3. Source-agnostic curated layer fed by staged unions/mappings.
4. Updated CI workflows to support multiple source pipelines.

#### Exit Criteria
- One additional source can be integrated without redesigning curated models.
- CI can validate source-specific and source-agnostic quality gates.

### Phase 3: Canonical Linking and Coverage Expansion (3-6 months)
#### Objectives
1. Expand coverage (more leagues/countries) while preserving consistency.
2. Make cross-source entity resolution a core project capability.

#### Deliverables
1. Canonical mapping tables with confidence metadata.
2. Coverage dashboard (by source, league, country, season, freshness).
3. Data contract checks for completeness, consistency, and timeliness.
4. Incremental onboarding of additional competitions and geographies.

#### Exit Criteria
- Multi-source data can be queried through one stable canonical schema.
- Coverage and data quality metrics are transparent and improving.

### Phase 4: Brand Migration and Ecosystem Consolidation (after platform stability)
#### Objectives
1. Complete identity transition without losing discoverability or trust.
2. Convert growth into ecosystem adoption.

#### Deliverables
1. Repository rename and communication package.
2. "Formerly transfermarkt-datasets" transitional branding period.
3. Migration guide for contributors and downstream users.
4. Contributor playbooks for adding new sources and tests.

#### Exit Criteria
- Community understands continuity and new scope.
- Adoption increases while update reliability remains high.

## Operating Principles
1. Backward compatibility first; breakage only with versioned migration paths.
2. Canonical schema stability is a product feature, not an afterthought.
3. Data lineage and quality must be observable by default.
4. Source diversity should increase trust, not reduce consistency.

## Immediate Next Iteration Candidates
1. Define canonical entity contracts (fields, keys, constraints).
2. Draft source onboarding checklist (technical + quality requirements).
3. Define KPI set for vision progress (coverage, freshness, reliability, adoption).
