Insert into $table_name
with wifi_networks as (
    select w.station_complex,
           'Yes' in (w.at_t, w.sprint, w.t_mobile, w.verizon) as wifi_provided,
           w.historical
    from $wifi_table w
    order by w.station_complex)
select s.transit_timestamp::date,
       w.historical,
       w.wifi_provided,
       sum(ridership) as daily_ridership_count
from $subway_table s
left join wifi_networks w
on s.station_complex = w.station_complex
where s.transit_timestamp::date = '$start_date'
group by s.transit_timestamp::date, w.historical, w.wifi_provided;