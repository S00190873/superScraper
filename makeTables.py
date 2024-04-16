import os
import pandas as pd
import psycopg2

dbname = 'ChartInformation'
user = 'postgres'
password = 'datapass'
host = 'localhost'
port = '5432'

csv_directory = 'C:/Users/jc20c/SuperScraper/Year End Chart Data For Database'

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

def create_table_from_df(df, table_name):
    columns = ", ".join([f"{col} TEXT" for col in df.columns if col != 'Genre'])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
    cursor.execute(create_table_query)

def import_csv_to_table(csv_path, table_name, all_songs):
    df = pd.read_csv(csv_path)

    genres = {
        "hot_dance_electronic_songs": "dance/electronic",
        "hot_rock_songs": "rock",
        "hot_country_songs": "country",
        "hot_r_and_b_songs": "r-and-b",
        "hot_rap_songs": "rap",
        "smooth_jazz_songs": "jazz",
        "classical_albums": "classical",
        "blues_albums": "blues",
        "hot-hard-rock-songs": "hard-rock"
    }

    simplified_table_name = table_name.rsplit('_', 1)[0]
    genre = genres.get(simplified_table_name, "none")
    df['Genre'] = genre

    all_songs = pd.concat([all_songs, df], ignore_index=True)

    create_table_from_df(df, table_name)
    columns = ", ".join([col for col in df.columns if col != 'Genre'])
    placeholders = ", ".join(["%s"] * (len(df.columns) - 1))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    for row in df.drop(columns=['Genre']).itertuples(index=False, name=None):
        cursor.execute(insert_query, row)

    return all_songs

all_songs = pd.DataFrame()

for filename in os.listdir(csv_directory):
    if filename.endswith('.csv'):
        table_name = os.path.splitext(filename)[0].replace("-", "_").lower()
        csv_path = os.path.join(csv_directory, filename)
        all_songs = import_csv_to_table(csv_path, table_name, all_songs)
        print(f"Imported {filename} into {table_name}")

all_songs['Chart Appearances'] = 1
all_songs = all_songs.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
all_songs = all_songs.sort_values(by=['Artist', 'Title', 'Genre'], ascending=[True, True, False])
all_songs['Genre'] = all_songs.groupby(['Artist', 'Title'])['Genre'].transform(lambda x: x.iloc[0])
all_songs = all_songs.groupby(['Artist', 'Title', 'Genre'], as_index=False).agg({'Chart Appearances': 'sum'})
all_songs = all_songs.sort_values(by='Chart Appearances', ascending=False)
all_songs.to_csv('C:/Users/jc20c/SuperScraper/all_songs_final.csv', index=False)

print(all_songs[all_songs['Genre'] == "none"][['Title', 'Artist']].to_csv('Songs-With-No-Genres.csv', index=False))

print(f"Number of duplicates found: {all_songs.duplicated(subset=['Title', 'Artist'], keep=False).sum()}")
cursor.close()
conn.close()