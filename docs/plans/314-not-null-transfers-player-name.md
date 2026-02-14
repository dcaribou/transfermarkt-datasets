# Fix #314: Inconsistent transfer data (`not_null_transfers_player_name`)

## Context

The `not_null_transfers_player_name` dbt test fails intermittently because:
- Transfer data comes from the **API** (`base_transfers`) — contains `player_id` but no player name
- Player names come from the **scraper** (`base_players` → `players`) — only covers first-tier leagues
- `transfers.sql` LEFT JOINs these two; when a player_id from the API has no match in the scraper data, `player_name` is NULL → test fails

**Key discovery:** The raw API JSON includes a `response.url` field (e.g., `/jordy-clasie/transfers/spieler/104223`) from which we can derive a fallback player name.

## Approach

Extract a fallback `player_name` from the API data in `base_transfers`, then use `COALESCE` in the curated `transfers` model to prefer the scraper name but fall back to the API-derived name.

## Changes

### 1. `dbt/models/base/transfermarkt_api/base_transfers.sql`

**`json_transfers` CTE** — add `response_url` extraction:
```sql
json_extract_string(json_row, '$.response.url') as response_url,
```

**`unnested` CTE** — carry `response_url` through:
```sql
select player_id, response_url, season, filename, unnest(...) as transfer
```

**`transfers_data` CTE** — derive `player_name` from URL slug:
```sql
nullif(
    array_to_string(
        list_transform(
            string_split(replace(split_part(response_url, '/', 2), '-', ' '), ' '),
            x -> upper(x[1]) || x[2:]
        ),
        ' '
    ),
    ''
) as player_name,
```
This converts `jordy-clasie` → `Jordy Clasie`. Verified working on DuckDB 1.4.4.

**`processed_transfers` CTE** — pass `player_name` through in the select list.

**Final SELECT** — add `player_name` to the output columns.

### 2. `dbt/models/curated/transfers.sql`

Change line 25 from:
```sql
players_cte.player_name
```
to:
```sql
coalesce(players_cte.player_name, transfers_cte.player_name) as player_name
```

### 3. No other changes needed

- `models.yml` — the `not_null` test stays as-is (it's the acceptance criterion)
- `players.sql` — unchanged
- No new tests needed

## Verification

```sh
cd dbt && ../.venv/bin/dbt build -s base_transfers transfers --target dev
```

Then confirm zero NULLs:
```sh
.venv/bin/python -c "import duckdb; print(duckdb.connect('dbt/duck.db').sql('SELECT COUNT(*) - COUNT(player_name) as null_names FROM dev.transfers').fetchall())"
```

Expected: `[(0,)]`
