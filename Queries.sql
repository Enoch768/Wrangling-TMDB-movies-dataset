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
create or replace procedure top_value (top_n int)
language plpgsql
as $$
begin 
select title, vote_count as vote_count
from movies join title on movies.title_id = title.id
order by vote_count limit top_n;
commit;
end;$$;