/* The goal is to normalize the data so there will be no vertical repitition of strings as that's one of the things to
take note in all sql database.All vertical repitition will be replaced with number which is id . So to do this, i intend
to put all facts data into different tables and connect all tables together. The first attempt is to put all rows in
json format into tables.*/

create table if not exists genres_and_movies(
genre jsonb, movie_id integer);
insert into genres_and_movies
select a.genres, a.id from tmdb_movies a where a.genres ->> 'name' is not null;
create table if not exists keywords(
keyword jsonb, movie_id integer);
insert into keywords
select a.kw, a.id from tmdb_movies a where a.kw ->>'name' is not null;
create table if not exists production_countries(
countries jsonb, movie_id integer);
insert into production_countries
select a.production_countries, a.id from tmdb_movies a where a.production_countries ->> 'name' is not null;
create table if not exists spoken_languages (
languages jsonb , movie_id integer);
insert into spoken_languages 
select a.spoken_languages, a.id from tmdb_movies a where a.spoken_languages ->> 'name' is not null;
create table if not exists movies(
id serial,
primary key(id),
budget bigint,
homepage text,
movie_id bigint,
original_language text,
original_title varchar(1500),
overview text,
popularity numeric,
release_date date,
revenue bigint,
runtime numeric,
status varchar(128),
tagline text,
vote_average numeric,
vote_count bigint);
insert into movies(
budget,homepage,movie_id,original_language,original_title,overview,popularity,release_date,revenue,
runtime,status,tagline,vote_average,vote_count)
select distinct budget,homepage,id,original_language,original_title,overview,popularity,release_date
,revenue,runtime,status,tagline,vote_average,vote_count from tmdb_movies
create table if not exists movie_cast(
casts jsonb, movie_id integer);
insert into movie_cast
select movie_casts, movie_id from tmdb_credits where movie_casts ->> 'name' is not null;
create table if not exists crew(
crew jsonb, movie_id integer);
insert into crew
select crew,movie_id from tmdb_credits where crew ->> 'name' is not null;
create table if not exists genres_list(
id serial,
primary key(id),
name varchar(1500));
insert into genres_list(name)
select distinct genre ->>'name' from genres_and_movies;
alter table genres_and_movies add column genre_id integer 
references genres_list(id) on delete cascade;
update genres_and_movies set genre_id = (select id from genres_list  where name = genre ->> 'name');
alter table genres_and_movies add column m_id integer
references movies(id) on delete cascade;
update genres_and_movies set m_id = (select id from movies where movie_id = genres_and_movies.movie_id);
alter table genres_and_movies drop column genre;
alter table genres_and_movies drop column movie_id;
create table kw_list(
word varchar(1500),
id serial,
primary key(id));
alter table keywords add column kw_id integer references kw_list(id) on delete cascade;
insert into kw_list(word)
select distinct keyword ->> 'name' from keywords;
update keywords set kw_id = (select id from kw_list where word = keyword ->> 'name')
alter table keywords add column m_id integer references movies(id) on delete cascade;
update keywords set m_id = (select id from movies where movie_id = keywords.movie_id);
alter table keywords drop column keyword;
alter table keywords drop column movie_id;
create table if not exists countries(
id serial,
primary key(id),
name varchar(500),
iso char(20));
insert into countries(name,iso)
select distinct countries ->> 'name', countries ->> 'iso_3166_1' from production_countries;
alter table production_countries add column country_id integer references countries(id) on delete cascade;
alter table production_countries add column m_id integer references movies(id) on delete cascade;
update production_countries set country_id = (select id from countries where name = 
											 production_countries.countries ->> 'name');
update production_countries set m_id = (select id from movies where movie_id = production_countries.movie_id);
alter table production_countries drop column countries;
alter table production_countries drop column movie_id;
create table if not exists languages(
name varchar(760),
id serial,
primary key(id),
iso char(20));
insert into languages(name,iso)
select DISTINCT languages ->> 'name', languages ->> 'iso_639_1' from spoken_languages;
alter table spoken_languages add column language_id integer references languages(id) on delete cascade;
alter table spoken_languages add column m_id integer references movies(id) on delete cascade;
alter table movies add column original_language_id integer references languages(id) on delete cascade;
update spoken_languages set language_id = (select id from languages where iso = spoken_languages.languages ->> 'iso_639_1');
update spoken_languages set m_id = (select id from movies where movie_id = spoken_languages.movie_id);
update movies set original_language_id = (select id from languages where name = original_language);
alter table movies drop column original_language;
alter table spoken_languages drop column languages;
alter table spoken_languages drop column movie_id;
create table original_title(
name varchar(500) unique,
id serial,
primary key(id));
insert into original_title(name)
select distinct original_title from movies;
alter table movies add column original_title_id integer references original_title(id) on delete cascade;
update movies set original_title_id = (select id from original_title where name = original_title);
alter table movies drop column original_title;
alter table movies add column title varchar(1500);
update movies as m set title = test.a from (select title,id from tmdb_movies) as test(a,b) where m.movie_id = test.b;
create table if not exists title(
id serial,
primary key(id),
name varchar(550) unique);
insert into title(name)
select distinct title from movies ;
alter table movies add column title_id integer references title(id) on delete cascade;
update movies set title_id = (select id from title where name = movies.title);
alter table movies drop column title;
create table if not exists companies (
id serial,
name varchar(700) unique,
primary key(id));
insert into companies(name)
select distinct production_company ->> 'name' from production_companies;
alter table production_companies add column company_id integer references companies(id) on delete cascade;
alter table production_companies add column m_id integer references movies(id) on delete cascade;
update production_companies as p set company_id = (select id from companies where name = production_company->>'name');
update production_companies as p set m_id = (select id from movies where movie_id = p.movie_id);
alter table production_companies drop column movie_id;
alter table production_companies drop column production_company;
create table crew_details(
id serial,
primary key(id),
name varchar(128),
gender integer);
insert into crew_details(name,gender)
select distinct crew ->> 'name', (crew->>'gender')::int from crew;
alter table crew add column name_id integer references crew_details(id) on delete cascade;
update crew as c set name_id = test.a from (select id , name from crew_details) as test(a,b) where test.b = c.crew->>'name';
create table department(
id serial,
primary key(id),
name varchar(300) unique);
insert into department (name)
select DISTINCT crew ->> 'department' from crew;
alter table crew add column department_id integer references department(id);
update crew set department_id = (select id from department where name = crew ->> 'department');

--create crew jobs table
create table if not exists job (
id serial,
primary key(id),
name varchar(90) unique);

insert into job(name)
select distinct crew ->> 'job' from crew;

alter table crew add column job_id integer references job(id);
update crew set job_id = (select id from job where name = crew ->> 'job');

--create relationship between movies table and crew table
alter table crew add column m_id integer references movies(id) on delete cascade;
update crew set m_id = (select id from movies where movie_id = crew.movie_id);
alter table crew drop column crew;
alter table crew drop column movie_id;

--put cast names in different table
create table if not exists cast_names (
id serial,
name varchar(128), --there's no use for unique constraint because more than two people can have same name
primary key(id),
gender integer);
insert into cast_names(name,gender)
select distinct casts ->> 'name' , (casts ->> 'gender')::int from movie_cast;

alter table movie_cast add column cast_name_id integer references casT_names(id);
update movie_cast as mc set cast_name_id = test.a from (
select id,name from cast_names) as test(a,b) where test.b = mc.casts ->> 'name';


--create table for character names
create table if not exists character_names(
id serial,
primary key(id),
name varchar(1500),
order_1 integer);
--insert distinct charcater from casts in movie_cast into character_names table
insert into character_names(name,order_1)
select distinct casts->>'character',(casts->>'order')::int from movie_cast;
--create relationship
alter table movie_cast add column character_id integer references character_names(id) on delete cascade;
update movie_cast as mc set character_id = test.a from (
select id,name from character_names) as test(a,b) where test.b = mc.casts ->> 'character';
--create relationship between movie_cast and movies tables
alter table movie_cast add column m_id integer references movies(id) on delete cascade;
update movie_cast set m_id = (select id from movies where movie_id = movie_cast.movie_id);
--drop columns that are no longer useful
alter table movie_cast drop column casts;
alter table movie_cast drop column movie_id;


/* Relaationship has been successfully created so i can go ahead and drop the lookup tables */
drop table tmdb_movies;
drop table tmdb_credits;