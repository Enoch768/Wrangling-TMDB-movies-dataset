<h1 align="center">
Introduction
</h1>
Data wrangling was carried out on 4803 tmdb movies dataset that was gathered from 4th of September 1916 till 3rd of February 2017.The dataset was gotten from Kaggle .
Before anything, i firstly performed  <b> normalization </b> so that there will be no vertical repitition of strings.
<br></br>
<h4> About data <h4>
There are two different datasets, the tmdb credits and the tmdb movies. The tmdb credits dataset contains the cast names , character names and crew data for each movie while tmdb movies dataset contain movies details.
<br></br>
<h4> About Project </h4>
I used python to insert the dataset to postgresql database with the python library use as a client fot postgresql database called <b> psycopg2 </b> . I iterate through all the rows im both dataset and unnested all objects in am array so that esch object will be in a single row amd then inserted them into the created table in jsonb form.
Normalization was then performed and the erd diagram is here .
Different queries were ran after the normalization.
