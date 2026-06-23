{#
    Verify the total number of first-tier leagues is within the expected range.
    COL1 (Colombia Liga BetPlay - Finalización) is seasonally inactive in Jun-Aug,
    so the count fluctuates between 31 (off-season) and 32 (full season).
#}

select count(*) as league_count
from {{ ref('competitions') }}
where sub_type = 'first_tier'
having count(*) not between 31 and 32
