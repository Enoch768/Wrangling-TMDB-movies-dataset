--total number of movies
select count(*) from movies;
--The maximun date and minimum date
select max(release_date) from movies;
select min(release_date) from movies;
--The maximum budget
select max(budget) from movies;
--The total number of casts
select count(*) from cast_names;
--total number of genres
select count(*) from genres_list;
--the maximum revenue
select max(revenue) from movies;
--the five longest running movie
select title.name, movies.runtime as runtime from movies 
join title on movies.title_id = title.id
where movies.runtime <> 'NaN'::numeric
order by runtime desc limit 5 ;
--five movies with the most budgets
select title.name, movies.budget as budget from movies
join title on movies.title_id = title.id
order by budget desc limit 5;
--top n movies by vote count
create function top_values(topn int)
returns table (title varchar(1500),
vote_count bigint)
language plpgsql as 
$body$ 
begin
	return query 
	select t.name,m.vote_count  from movies m
	join title t on m.title_id = t.id
	order by m.vote_count desc
	limit topn;
end
$body$;
select * from top_values(5);

--how many production companies
select count(*) from companies;
--company with most movies
select companies.name , count(m_id) as m_id_count
from production_companies join companies on
production_companies.company_id = companies.id
group by companies.name
order by m_id_count desc limit 1;
--the budget of top 20 movies of the comapny with most movies by popularity
select companies.name, title.name, movies.budget, movies.popularity
from movies 
join title on movies.title_id = title.id
join production_companies on movies.id = production_companies.m_id
join companies on production_companies.company_id = companies.id
where companies.name = 'Warner Bros.'
order by movies.popularity desc limit 20;
--count of each genre in alphabetical order
select genres_list.name , count(m_id)
from genres_and_movies join genres_list
on genres_and_movies.genre_id = genres_list.id
group by genres_list.name
order by genres_list.name ;
--analysis on movies with more than 5 genres
SELECT title, avg(popularity) as popularity,avg(vote_count) as vote_count,avg(vote_average) as vote_average
from movies join genres_and_movies on movies.id = genres_and_movies.m_id 
join title on movies.title_id = title.id group by title having count(title) > 5
order by popularity limit 10;
--how many movies has over 4 spoken languages 
select count(*) from (
  select ft.title,count(ft.title) as c_title from(
      select movies.id as m_id,title.name as title
         from movies join title on movies.title_id = title.id) as ft
       join spoken_languages sl on ft.m_id = sl.m_id group by ft.title) as ft2
where c_title > 4;
--further analysis on them
select * from (
  select ft.title,count(ft.title) as languages_count,
  sum(ft.popularity) as popularity, sum(ft.vote_average) as vote_average,
    sum(ft.vote_count) as vote_count 
    from(
      select movies.id as m_id,title.name as title, 
      		movies.popularity as popularity, movies.vote_average as vote_average , movies.vote_count as vote_count
         from movies join title on movies.title_id = title.id) as ft
       join spoken_languages sl on ft.m_id = sl.m_id group by ft.title) as ft2
where languages_count > 4;
--is there any actor that play two roles in a movie
select * from (
SELECT title.name, cast_names.name,count(distinct character_names.name) from movie_cast 
join movies on movie_cast.m_id = movies.id 
join title on movies.title_id = title.id 
join cast_names on movie_cast.cast_name_id = cast_names.id 
join character_names on movie_cast.character_id = character_names.id
group by title.name,cast_names.name,character_names."name"  having count(distinct character_names."name") > 0) as ft;
/* Time series analysis */
--Analysis on seasonality for each year
select sum(popularity),sum(vote_count),sum(vote_average),
to_char(release_date,'YYYY') as year,
case
	when to_char(release_date, 'mon') in ('mar','apr','may') then 'Spring'
	when to_char(release_date, 'mon') in ('jun','jul','august') then 'Summer'
	when to_char(release_date, 'mon') in ('sep','oct','nov') then 'Fall'
	else 'Winter'
end as season
from movies
group by year,season
order by year;
--genres count for each seasons of the year
select ft2.year ,ft2.season,genres_list.name as genre_name,count(genres_list.name)as genre_c from (
select * from (
select 
to_char(release_date,'YYYY') as year,
case
	when to_char(release_date, 'mon') in ('mar','apr','may') then 'Spring'
	when to_char(release_date, 'mon') in ('jun','jul','august') then 'Summer'
	when to_char(release_date, 'mon') in ('sep','oct','nov') then 'Fall'
	else 'Winter'
end as season,
id as m_id
from movies) as ft
join genres_and_movies on genres_and_movies.m_id = ft.m_id) as ft2
join genres_list on genres_list.id = ft2.genre_id
group by ft2.season,ft2.year,genres_list.name order by ft2.year desc;
--YOY and MOM budgets and popularity for movies produced in usa
select p_year,yearly_budget ,yearly_popularity,
lag(yearly_budget) over (order by p_year) as previous_year_budget,
lag(yearly_popularity) over (order by p_year) as previous_year_popularity
from (select 
to_char(release_date, 'YYYY') as p_year,
sum(budget) as yearly_budget,
sum(popularity) as yearly_popularity 
from 
movies 
join production_countries pc on movies.id = pc.m_id 
join countries c on pc.country_id = c.id 
where c.iso = 'US'
group by p_year) as ft;