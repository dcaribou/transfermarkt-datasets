select *
from {{ ref('base_countries') }}
order by country_id
