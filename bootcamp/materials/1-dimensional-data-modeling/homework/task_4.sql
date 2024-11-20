do $$
declare date_asofyear integer;
begin
		date_asofyear := 1970;

MERGE INTO actors_history_scd ascd USING (
with with_previous as (
    select
        actor,
        actorid,
        asofyear,
        films,
        quality_class,
        is_active,
        lag(quality_class, 1)
            over (partition by actorid order by asofyear)
        as previous_quality_class,
        lag(is_active, 1)
            over (partition by actorid order by asofyear)
        as previous_is_active


    from actors
    where asofyear <= date_asofyear
),

with_indicators as (
    select
        *,
        case
            when is_active <> previous_is_active then 1
            when quality_class <> previous_quality_class then 1
            else 0
        end as change_indicator
    from with_previous
),

with_streaks as (
    select
        *,
        sum(change_indicator)
            over (partition by actorid order by asofyear)
        as streak_identifier
    from with_indicators
)

select
actor,
    actorid,
    quality_class,
    date_asofyear as asofyear,
        is_active,
    min(asofyear) as start_year,
    max(asofyear) as end_year
from with_streaks
group by actor, actorid, is_active, quality_class
order by actor, start_year
) as source ON (source.actorid = ascd.actorid AND source.asofyear = ascd.asofyear)
WHEN MATCHED THEN
    UPDATE SET
    actor = source.actor,
    is_active = source.is_active,
    quality_class = source.quality_class,
    start_date = source.start_year,
    end_date = source.end_year

WHEN NOT MATCHED THEN  INSERT(
    actor, actorid, quality_class, asofyear, is_active, start_date, end_date
)
VALUES(
    source.actor, source.actorid, source.quality_class, source.asofyear, source.is_active, source.start_year, source.end_year
);
end $$;
