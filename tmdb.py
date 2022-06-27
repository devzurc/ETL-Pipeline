"""
    Sup!
    This is my ETL pipeline following the website instructions below: 
    https://towardsdev.com/create-an-etl-pipeline-in-python-with-pandas-in-10-minutes-6be436483ec9
"""

import pandas as pd
import requests
import config


__author__ = "devzurc"
__date__ = "May 26 2022"

resp_list = []
API_KEY = config.api_key

# Let's see 5 movies informations;
for movie_id in range(550, 556):
    url = f'https://api.themoviedb.org/3/movie/{movie_id}?api_key={API_KEY}'

    r = requests.get(url)
    resp_list.append(r.json())

df = pd.DataFrame.from_dict(resp_list)

# Columns filter;
df_columns = ['budget', 'genres', 'id', 'imdb_id', 'original_title', 'release_date', 'revenue', 'runtime']
genres_list = df['genres'].tolist()
flat_list = [item for sublist in genres_list for item in sublist]

result = []

for l in genres_list:
    r = []

    for d in l:
        r.append(d['name'])
        
    result.append(r)

df = df.assign(genres_all=result)

df_genres = pd.DataFrame.from_records(flat_list).drop_duplicates()
df_genre_columns = df_genres['name'].to_list()
df_columns.extend(df_genre_columns)

s = df['genres_all'].explode()
df = df.join(pd.crosstab(s.index, s))

df['release_date'] = pd.to_datetime(df['release_date'])
df['day'] = df['release_date'].dt.day
df['month'] = df['release_date'].dt.month
df['year'] = df['release_date'].dt.year
df['day_of_week'] = df['release_date'].dt.day_name()
df_time_columns = ['id', 'release_date', 'day', 'month', 'year', 'day_of_week']

df[df_time_columns]
# %%
