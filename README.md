<h1 align="center">
Introduction
</h1>
Data wrangling was carried out on 4803 tmdb movies dataset that was gathered from 4th of September 1916 till 3rd of February 2017.The dataset was gotten from Kaggle .
Before anything, i firstly performed  <b> normalization </b> so that there will be no vertical repitition of strings.
<br></br>
<h4> About data </h4>
There are two different datasets, the tmdb credits and the tmdb movies. The tmdb credits dataset contains the cast names , character names and crew data for each movie while tmdb movies dataset contain movies details.
<br></br>
<h5> Data dictionary </h5>
<h6> TMDB movies dataset </h6>
<ul>
<list> homepage- URL to the movie. </list>

<list> id - integer movie id which is unique to all movies. </list>

<list> riginal_title - text , the original title of a movie. </list>

<list> overview - text , movie description .</list>

<list> popularity - integer - how popular a movie is. </list>

<list> production_companies - Companies in charge of yhe movie production in array of objects format. </list>

<list> production_countries - Countries where the movie is produced in areay of objects format as well. </list>

<list> release_date - release date of a movie. </list>

<list> spoken_languages - array of objects of languages spoken in a movie . </list>

<list> status - text , show if movie is released or not </list>

<list> tagline - text </list>

<list> vote_average - float, average of votes </list>
<list> vote_count - integer, total votes </list>
</ul>
<h6> Tmdb credits dataset </h6>
<ul>
<list> Crew - Array of objects that contain crew that partake in the production of a movie, their gender, their department.</list>

<list> movie_id - integer references
id on tmdb movie dataset . </list>

<list> cast - array of objects that contain names of casts im a movie and the role they played. </list>
</ul>

<h4> About Project </h4>
I used python to insert the dataset to postgresql database with the python library use as a client fot postgresql database called <b> psycopg2 </b> . I iterate through all the rows im both dataset and unnested all objects in an array so that esch object will be in a single row , converted them to json dsta and then inserted them into the created table in jsonb form.
Normalization was then performed and the erd diagram is <a href = https://github.com/Enoch768/Wrangling-TMDB-movies-dataset/blob/main/tmdb.png> here </a> .
Different queries were ran after the normalization.
