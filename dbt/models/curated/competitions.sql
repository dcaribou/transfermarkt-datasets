select 
    *,
    case when competition_id in (
        'ES1', 'GB1', 'IT1', 'FR1', 'L1'
    ) then true
    else false
    end as is_major_national_league
from {{ ref('base_competitions') }}
