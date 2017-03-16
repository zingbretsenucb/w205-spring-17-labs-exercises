-- States have enough data that I feel comfortable averaging
-- the z-scores of all the measures for each state
-- to generate the rankings

drop table best_states;

create table best_states as select h.state,
	avg(e.score) as score, count(e.score) as n 
	from all_measures e, hospitals h 
	where e.provider_id = h.provider_id 
	group by h.state ;
    
select * from best_states order by score DESC limit 10;
