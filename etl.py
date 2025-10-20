import os
import time
import json
import re
import pandas as pd
import requests
import pyodbc
from dotenv import load_dotenv
from tqdm import tqdm

# Loading the environment variable

load_dotenv()

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_DRIVER = os.getenv("DB_DRIVER")
DB_TRUSTED = os.getenv("DB_TRUSTED", "yes")
DB_ENCRYPT = os.getenv("DB_ENCRYPT", "yes")
DB_TRUST_CERT = os.getenv("DB_TRUST_CERT", "yes")


# Building the connection string for the database

if DB_TRUSTED.lower() == "yes":
    conn_str = (
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"Trusted_Connection=yes;"
        f"Encrypt={DB_ENCRYPT};"
        f"TrustServerCertificate={DB_TRUST_CERT};"
    )
else:
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    conn_str = (
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};PWD={DB_PASS};"
        f"Encrypt={DB_ENCRYPT};"
        f"TrustServerCertificate={DB_TRUST_CERT};"
    )

# connecting to the SQL server management server studio for further queries

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
print("✅ Connected to SQL Server successfully.")


# Settign up the CACHE for OMDb

CACHE_FILE = "omdb_cache.json"
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        omdb_cache = json.load(f)
else:
    omdb_cache = {}

# Helper function to fetch the data for processign the data 


def query_omdb(title, year=None):
    key = f"{title}::{year}" if year else title
    if key in omdb_cache:
        return omdb_cache[key]

    params = {"apikey": OMDB_API_KEY, "t": title}
    if year:
        params["y"] = str(year)

    resp = requests.get("http://www.omdbapi.com/", params=params)
    if resp.status_code != 200:
        print(f"OMDb error for {title}")
        return None

    data = resp.json()
    if data.get("Response") == "False":
        omdb_cache[key] = None
        return None

    omdb_cache[key] = data
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(omdb_cache, f, ensure_ascii=False, indent=2)
    time.sleep(0.2)
    return data

def parse_title(title_str):
    match = re.match(r"^(.*)\s+\((\d{4})\)$", title_str.strip())
    if match:
        return match.group(1), int(match.group(2))
    return title_str, None

def insert_movie(movie_id, title, year, omdb):
    imdb_id = director = plot = box_office = runtime_minutes = language = country = None
    if omdb:
        imdb_id = omdb.get("imdbID")
        director = omdb.get("Director")
        plot = omdb.get("Plot")
        box_office = omdb.get("BoxOffice")
        runtime = omdb.get("Runtime")
        if runtime and runtime.endswith("min"):
            try:
                runtime_minutes = int(runtime.split()[0])
            except:
                runtime_minutes = None
        language = omdb.get("Language")
        country = omdb.get("Country")

    cursor.execute("""
        MERGE Movies AS target
        USING (SELECT ? AS movie_id) AS src
        ON target.movie_id = src.movie_id
        WHEN MATCHED THEN 
            UPDATE SET 
                title = ?,
                year = ?,
                imdb_id = COALESCE(?, target.imdb_id),
                director = COALESCE(?, target.director),
                plot = COALESCE(?, target.plot),
                box_office = COALESCE(?, target.box_office),
                runtime_minutes = COALESCE(?, target.runtime_minutes),
                language = COALESCE(?, target.language),
                country = COALESCE(?, target.country),
                last_updated = GETDATE()
        WHEN NOT MATCHED THEN 
            INSERT (movie_id, title, year, imdb_id, director, plot, box_office, runtime_minutes, language, country)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, (movie_id, title, year, imdb_id, director, plot, box_office, runtime_minutes,
          language, country, movie_id, title, year, imdb_id, director, plot, box_office,
          runtime_minutes, language, country))
    conn.commit()

def ensure_genre(genre_name):
    cursor.execute("SELECT genre_id FROM Genres WHERE name = ?", (genre_name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("INSERT INTO Genres (name) VALUES (?)", (genre_name,))
    conn.commit()
    return cursor.execute("SELECT genre_id FROM Genres WHERE name = ?", (genre_name,)).fetchone()[0]

def link_movie_genres(movie_id, genres):
    for g in genres:
        gid = ensure_genre(g)
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM MovieGenres WHERE movie_id = ? AND genre_id = ?)
            INSERT INTO MovieGenres (movie_id, genre_id) VALUES (?, ?)
        """, (movie_id, gid, movie_id, gid))
    conn.commit()

def load_ratings(df):
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO Ratings (user_id, movie_id, rating, timestamp)
            VALUES (?, ?, ?, ?)
        """, (int(row['userId']), int(row['movieId']), float(row['rating']), int(row['timestamp'])))
    conn.commit()

#The main login is here to Extract Transform and Load the data

 
def main():
    print("Reading CSVs...")
    movies_df = pd.read_csv("movies.csv")
    ratings_df = pd.read_csv("ratings.csv")

    print("Processing movies...")
    for _, row in tqdm(movies_df.iterrows(), total=movies_df.shape[0]):
        movie_id = int(row["movieId"])
        title_raw = row["title"]
        title, year = parse_title(title_raw)
        genres = [g.strip() for g in row["genres"].split("|") if g != "(no genres listed)"]

        omdb = query_omdb(title, year)
        insert_movie(movie_id, title_raw, year, omdb)
        link_movie_genres(movie_id, genres)

    print("Loading ratings...")
    load_ratings(ratings_df)

    print("✅ ETL Completed Successfully!")

if __name__ == "__main__":
    main()
