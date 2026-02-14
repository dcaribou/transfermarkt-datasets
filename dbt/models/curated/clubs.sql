with clubs_cte as (

    select * from {{ ref('base_clubs') }}

)

select *

from clubs_cte

order by club_id
