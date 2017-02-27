-- Since all of the hospitals didn't have the same number of ratings
-- I decided not to use the average of the z-scored outcomes.
-- A hospital with one rating could end up with a high ranking by chance.
-- I decided to use the sum of the z-scored ratings instead.
-- Because z-scores are mean-centered around 0, hospitals should end up
-- with summed z-scores around 0 if the scores were assigned by chance.
-- Hospitals that consistently perform better than chance are rewarded
-- with higher ratings

drop table best_hospitals;

create table best_hospitals as select h.provider_id, 
	h.hospital_name, sum(e.score) as score, 
	count(e.score) as n 
	from all_measures e, hospitals h 
	where e.provider_id = h.provider_id 
	group by h.provider_id, h.hospital_name;

select * from best_hospitals order by score DESC limit 10;
