{#
    Verify the total number of first-tier leagues matches the expected count.
    Should be updated when new leagues are added to competitions.json.
#}

select count(*) as league_count
from {{ ref('competitions') }}
where sub_type = 'first_tier'
having count(*) != 32
