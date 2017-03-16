-- Compute standard deviation of scaled (not z-scored) outcome measures

drop table measure_variability;

create table measure_variability as 
    select a.measure_id, m.measure_name, 
    stddev(a.score_scaled) as std 
    from all_measures a, measures m
    where a.measure_id = m.measure_id
    group by a.measure_id, m.measure_name;

select * from measure_variability
    order by std DESC limit 10;
