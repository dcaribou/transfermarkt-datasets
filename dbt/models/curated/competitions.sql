select *
from {{ ref('base_competitions') }}

order by competition_id
