import psycopg2 as psql #python library to connect to postgresql databse
import pandas as pd #to load dataset
import json 
import itertools as iter
import re

#load datasets
df_credits = pd.read_csv('tmdb_5000_credits.csv')
df_movies = pd.read_csv('tmdb_5000_movies.csv')

#create connection
conn = psql.connect(
    database='tmdb',
    user='tmdb',
    password='password',
    host='127.0.0.1',
    port='5432'
)
cur = conn.cursor()
drop_t = 'drop table if exists tmdb_credits'
cur.execute(drop_t)
drop_t2 = 'drop table if exists tmdb_movies' #for anytime i run into an exception
cur.execute(drop_t2)
conn.commit()

#create tables 
sql = '''
CREATE TABLE tmdb_credits(
    movie_id integer,
    title varchar(1500),
    movie_casts jsonb,
    crew jsonb
)
'''
cur.execute(sql)
conn.commit()
sql2 = '''
create table tmdb_movies(
    budget bigint,
    genres jsonb,
    homepage text,
    id bigint,
    keywords jsonb,
    original_language text,
    original_title varchar(1500),
    overview text,
    popularity numeric,
    production_countries jsonb,
    release_date date,
    revenue bigint,
    runtime numeric,
    spoken_languages jsonb,
    status varchar(128),
    tagline text,
    title varchar(1500),
    vote_average numeric,
    vote_count bigint
)
'''
cur.execute(sql2)
conn.commit()
#make sure release date is in date format
df_movies['release_date'].fillna('2006-01-01', inplace=True)
df_movies['release_date'] = pd.to_datetime(df_movies['release_date'])

#insert values into the tables 
total_row = 0
for i in range(len(df_credits)):
    df = df_credits.iloc[i]
    movie_id = int(df['movie_id'])
    title = df['title']
    cast = df['cast']
    cast = cast.replace('[','')
    cast = cast.replace(']','')
    split_cast = cast.split('},')
    crew = df['crew']
    crew = crew.replace('[','')
    crew = crew.replace(']','')
    split_crew = crew.split('},')
    for (a,b) in iter.zip_longest(split_cast,split_crew,fillvalue='{ }'):
        sql = '''
        insert into tmdb_credits values (%s,%s,%s,%s)
        '''
        if re.findall('}$', a):
            cast = json.loads(a)
        else:
            try:
                cast = json.loads(a+'}')
            except:
                cast = json.dumps([a+'}'])
                cast = json.loads(cast)
        if re.findall('}$', b):
            crew = json.loads(b)
        else:
            try:
                crew = json.loads(b+'}')
            except:
                crew = json.dumps([b+'}'])
                crew = json.loads(crew)
        cur.execute(sql,(movie_id,title,json.dumps(cast),json.dumps(crew)))
        total_row += 1
        conn.commit()
print(f'Inserted {total_row} into tmdb credits table')
total_rows2 = 0
for i in range(len(df_movies)):
    df2 = df_movies.iloc[i]
    budget = int(df2['budget'])
    genre = df2['genres']
    genre = genre.replace('[','')
    genre = genre.replace(']','')
    split_genre = genre.split('},')
    hp = df2['homepage']
    id = int(df2['id'])
    kw = df2['keywords']
    kw = kw.replace('[','')
    kw = kw.replace(']','')
    split_kw = kw.split('},')
    original_l = df2['original_language']
    original_title = df2['original_title']
    overview = df2['overview']
    popu = float(df2['popularity'])
    p_c2 = df2['production_countries']
    p_c2 = p_c2.replace('[','')
    p_c2 = p_c2.replace(']','')
    split_p_c2 = p_c2.split('},')
    release_date = df2['release_date']
    revenue =int(df2['revenue'])
    runtime = float(df2['runtime'])
    spoken_l = df2['spoken_languages']
    spoken_l = spoken_l.replace('[','')
    spoken_l = spoken_l.replace(']','')
    split_spoken1 = spoken_l.split('},')
    status = df2['status']
    tagline = df2['tagline']
    title = df2['title']
    vote_average = float(df2['vote_average'])
    vote_count = int(df2['vote_count'])
    for (a,b,d,e) in iter.zip_longest(split_genre,split_kw,split_p_c2,split_spoken1,fillvalue='{ }'):
        if re.findall('}$', a):
            genre = json.loads(a)
        else:
            try:
                genre = json.loads(a+'}')
            except:
                genre = json.dumps([a+'}'])
                genre = json.loads(genre)
        if re.findall('}$', b):
            kw = json.loads(b)
        else:
            try:
                kw = json.loads(b+'}')
            except:
                kw = json.dumps([b+'}'])
                kw = json.loads(kw)
        if re.findall('}$', d):
            country = json.loads(d)
        else:
            try:
                country = json.loads(d+'}')
            except:
                country = json.dumps([d+'}'])
                country = json.loads(country)
        if re.findall('}$', e):
            language2 = json.loads(e)
        else:
            try:
                language2 = json.loads(e+'}')
            except:
                language2 = json.dumps([e+'}'])
                language2 = json.loads(language2)
        sql = '''
        insert into tmdb_movies values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''
        cur.execute(sql,(
            budget,
            json.dumps(genre),
            hp,
            id,
            json.dumps(kw),
            original_l,
            original_title,
            overview,
            popu,
            json.dumps(country),
            release_date,
            revenue,
            runtime,
            json.dumps(language2),
            status,
            tagline,
            title,
            vote_average,
            vote_count
        ))
        total_rows2 += 1
        conn.commit()
print(f' inserted {total_rows2} rows into tmdb movies table')
#create production companies table and insert data
total_rows3 = 0
cur.execute('drop table if exists production_companies')
cur.execute('create table production_companies (movie_id integer,production_company jsonb)')
for i in range(len(df_movies)):
    df = df_movies.iloc[i]
    movie_id = int(df['id'])
    cast = df['production_companies']
    cast = cast.replace('[','')
    cast = cast.replace(']','')
    split_cast = cast.split('},')
    for a in split_cast:
        if re.findall('}$', a):
            company = json.loads(a)
        else:
            try:
                company = json.loads(a+'}')
            except:
                pass
        cur.execute('insert into production_companies values (%s,%s)',(movie_id,json.dumps(company)))
        total_rows3 += 1
        conn.commit()
print(f'Inserted {total_rows2} into production companies table')
print('Commiting all changes...............')
conn.commit()
print('All changes Committed!!!')
cur.close()
print('Connection Closed')

