with
    time_series as (
        select date_time
        from unnest(
           {{ generate_timestamp_big_array('2010-12-30', '2011-07-01') }}
        ) as date_time
    )
    , dates as (
        select
            datetime_trunc(cast(date_time as datetime), minute) as date_time
            , extract(date from date_time) as day_date
            , date_trunc(date_time, week(monday)) as monday_date_week
            , date_trunc(date_time, month) as date_month
            , extract(day from date_time) as dayofmonth
            , extract(month from date_time) as month
            , extract(year from date_time) as year
            , extract(quarter from date_time) as quarter
            , extract(dayofyear from date_time) as dayofyear
            , extract(week from date_time) as isoweek
            , extract(week(monday) from date_time) as monday_week_number
            , case
                when extract(dayofweek from date_time) = 1 then 'Domingo'
                when extract(dayofweek from date_time) = 2 then 'Segunda'
                when extract(dayofweek from date_time) = 3 then 'Terça'
                when extract(dayofweek from date_time) = 4 then 'Quarta'
                when extract(dayofweek from date_time) = 5 then 'Quinta'
                when extract(dayofweek from date_time) = 6 then 'Sexta'
                when extract(dayofweek from date_time) = 7 then 'Sábado'
            end as day_name
            , cast(format_date("%u", date_time) as int) as dayofweek
            , extract(time from datetime(date_time)) as time
            , extract(hour from datetime(date_time)) as hour
            , case
                when extract(hour from datetime(date_time)) >= 0 and extract(hour from datetime(date_time)) < 6 then 'Madrugada'
                when extract(hour from datetime(date_time)) >= 6 and extract(hour from datetime(date_time)) < 12 then 'Manhã'
                when extract(hour from datetime(date_time)) >= 12 and extract(hour from datetime(date_time)) < 18 then 'Tarde'
                when extract(hour from datetime(date_time)) >= 18 and extract(hour from datetime(date_time)) < 24 then 'Noite'
            end as day_shift
            , case 
                when cast(format_date("%m", date_time) as int) = 1 then 'Janeiro'
                when cast(format_date("%m", date_time) as int) = 2 then 'Fevereiro'
                when cast(format_date("%m", date_time) as int) = 3 then 'Março'
                when cast(format_date("%m", date_time) as int) = 4 then 'Abril'
                when cast(format_date("%m", date_time) as int) = 5 then 'Maio'
                when cast(format_date("%m", date_time) as int) = 6 then 'Junho'
                when cast(format_date("%m", date_time) as int) = 7 then 'Julho'
                when cast(format_date("%m", date_time) as int) = 8 then 'Agosto'
                when cast(format_date("%m", date_time) as int) = 9 then 'Setembro'
                when cast(format_date("%m", date_time) as int) = 10  then 'Outubro'
                when cast(format_date("%m", date_time) as int) = 11  then 'Novembro'
                when cast(format_date("%m", date_time) as int) = 12  then 'Dezembro'
                else null
            end as month_name
            , case 
                when cast(format_date("%m", date_time) as int) = 1 then 'Jan'
                when cast(format_date("%m", date_time) as int) = 2 then 'Fev'
                when cast(format_date("%m", date_time) as int) = 3 then 'Mar'
                when cast(format_date("%m", date_time) as int) = 4 then 'Abr'
                when cast(format_date("%m", date_time) as int) = 5 then 'Mai'
                when cast(format_date("%m", date_time) as int) = 6 then 'Jun'
                when cast(format_date("%m", date_time) as int) = 7 then 'Jul'
                when cast(format_date("%m", date_time) as int) = 8 then 'Ago'
                when cast(format_date("%m", date_time) as int) = 9 then 'Set'
                when cast(format_date("%m", date_time) as int) = 10  then 'Out'
                when cast(format_date("%m", date_time) as int) = 11  then 'Nov'
                when cast(format_date("%m", date_time) as int) = 12  then 'Dez'
                else null
            end as month_name_initials
            , concat(format_date("%Q", date_time),'º Tri') AS quarter_initials
            , case 
                when cast(format_date("%m", date_time) as int) = 1  then concat(format_date("%d",date_time),'/Jan')
                when cast(format_date("%m", date_time) as int) = 2  then concat(format_date("%d",date_time),'/Fev')
                when cast(format_date("%m", date_time) as int) = 3  then concat(format_date("%d",date_time),'/Mar')
                when cast(format_date("%m", date_time) as int) = 4  then concat(format_date("%d",date_time),'/Abr')
                when cast(format_date("%m", date_time) as int) = 5  then concat(format_date("%d",date_time),'/Mai')
                when cast(format_date("%m", date_time) as int) = 6  then concat(format_date("%d",date_time),'/Jun')
                when cast(format_date("%m", date_time) as int) = 7  then concat(format_date("%d",date_time),'/Jul')
                when cast(format_date("%m", date_time) as int) = 8  then concat(format_date("%d",date_time),'/Ago')
                when cast(format_date("%m", date_time) as int) = 9  then concat(format_date("%d",date_time),'/Set')
                when cast(format_date("%m", date_time) as int) = 10 then concat(format_date("%d",date_time),'/Out')
                when cast(format_date("%m", date_time) as int) = 11 then concat(format_date("%d",date_time),'/Nov')
                when cast(format_date("%m", date_time) as int) = 12 then concat(format_date("%d",date_time),'/Dez')
                else null
            end as day_month
            , case 
                when cast(format_date("%m", date_time) as int) = 1  then concat('Jan', '/',format_date("%y", date_time))
                when cast(format_date("%m", date_time) as int) = 2  then concat('Fev', '/',format_date("%y", date_time))
                when cast(format_date("%m", date_time) as int) = 3  then concat('Mar', '/',format_date("%y", date_time))
                when cast(format_date("%m", date_time) as int) = 4  then concat('Abr', '/',format_date("%y", date_time))
                when cast(format_date("%m", date_time) as int) = 5  then concat('Mai', '/',format_date("%y", date_time))
                when cast(format_date("%m", date_time) as int) = 6  then concat('Jun', '/',format_date("%y", date_time))
                when cast(format_date("%m", date_time) as int) = 7  then concat('Jul', '/',format_date("%y", date_time))
                when cast(format_date("%m", date_time) as int) = 8  then concat('Ago', '/',format_date("%y", date_time))
                when cast(format_date("%m", date_time) as int) = 9  then concat('Set', '/',format_date("%y", date_time))
                when cast(format_date("%m", date_time) as int) = 10 then concat('Out', '/',format_date("%y", date_time))
                when cast(format_date("%m", date_time) as int) = 11 then concat('Nov', '/',format_date("%y", date_time))
                when cast(format_date("%m", date_time) as int) = 12 then concat('Dez', '/',format_date("%y", date_time))
                else null
            end as month_year
        from time_series
    )
select *
from dates
