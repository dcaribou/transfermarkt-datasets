{#
    Every first-tier league should have at least as many clubs in the dataset
    as the league's official club count (total_clubs from the scraper).
    Also checks that every league has total_clubs defined.
#}

with league_clubs as (
    select
        c.competition_id,
        c.name,
        c.total_clubs as expected_clubs,
        count(distinct cl.club_id) as actual_clubs
    from {{ ref('competitions') }} c
    left join {{ ref('clubs') }} cl
        on cl.domestic_competition_id = c.competition_id
    where c.sub_type = 'first_tier'
    group by 1, 2, 3
)

select *
from league_clubs
where expected_clubs is null
   or actual_clubs < expected_clubs
