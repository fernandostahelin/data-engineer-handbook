create type actors_scd_type as (
    quality_class quality_class [],
    is_active BOOLEAN,
    start_date integer,
    end_date integer
);

with last_year_scd as (
    select *
    from actors_history_scd
    where
        asofyear = 2020
        and end_date = 2020
),

historical_scd as (
    select
        actor,
        quality_class,
        is_active,
        start_date,
        end_date
    from actors_history_scd
    where
        asofyear = 2020
        and end_date <= 2020
),

this_year_data as (
    select *
    from actors
    where asofyear = 2021
),

unchanged_records as (
    select
        ts.actor,
        ts.quality_class,
        ts.is_active,
        ls.start_date,
        ts.asofyear as end_date
    from this_year_data as ts
    inner join last_year_scd as ls on ts.actorid = ls.actorid
    where
        ls.quality_class = ts.quality_class
        and ls.is_active = ts.is_active
),

changed_records as (
    select
        ts.actor,
        unnest(
            array[
                row(
                    ls.quality_class,
                    ls.is_active,
                    ls.start_date,
                    ls.end_date
                )::actors_scd_type,

                row(
                    ts.quality_class,
                    ts.is_active,
                    ts.asofyear,
                    ts.asofyear
                )::actors_scd_type
            ]
        ) as records
    from this_year_data as ts
    left join last_year_scd as ls on ts.actorid = ls.actorid
    where (
        ts.quality_class <> ls.quality_class
        or ts.is_active <> ls.is_active
    )
),

unnested_changed_records as (
    select
        actor,
        (records::actors_scd_type).quality_class,
        (records::actors_scd_type).is_active,
        (records::actors_scd_type).start_date,
        (records::actors_scd_type).end_date
    from changed_records
),

new_records as (
    select
        ts.actor,
        ts.quality_class,
        ts.is_active,
        ts.asofyear as start_date,
        ts.asofyear as end_date
    from this_year_data as ts
    left join last_year_scd as ls on ts.actorid = ls.actorid
    where ls.actorid is NULL
)

select *
from historical_scd
union all
select *
from unchanged_records
union all
select *
from unnested_changed_records
union all
select *
from new_records;
