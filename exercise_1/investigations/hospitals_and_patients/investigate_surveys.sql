
-- Load in all measures (effective care and readmissions)
drop table agg_scores;
create table agg_scores as select provider_id, 
	sum(score) as sum_score, 
	stddev(score) as stddev_score 
	from all_measures group by provider_id;

-- Join measures with survey responses
drop table mean_surveys;
create table mean_surveys as select a.provider_id, a.sum_score,
	a.stddev_score, s.base_score, s.consistency 
	from agg_scores a, surveys s 
	where a.provider_id = s.provider_id;

-- Compute correlations of z-scored outcome measures 
-- with patient satisfaction measures
drop table correlations;
create table correlations as 
select corr(sum_score, base_score) as sum_base,
	corr(sum_score, consistency) as sum_const,
	corr(stddev_score, base_score) as stddev_base,
	corr(stddev_score, consistency) as stddev_const
	from mean_surveys;

drop table mean_surveys;
drop table agg_scores;

select * from correlations;
