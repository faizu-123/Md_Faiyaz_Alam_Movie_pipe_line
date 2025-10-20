# 🎬 Movie Data Engineering Project

This project is part of my Data Engineering assignment. It demonstrates how to build an end-to-end ETL (Extract, Transform, Load) pipeline using Python, SQL Server, and the OMDb API. The goal is to collect, clean, enrich, and load movie data into a relational database and then run analytical SQL queries on it.

---

## 📘 Overview

The pipeline performs the following tasks:

1. Extract:  
   - Reads `movies.csv` and `ratings.csv` from the [MovieLens dataset](https://grouplens.org/datasets/movielens/latest/).  
   - Fetches additional movie details like director, plot, runtime, and language using the OMDb API.

2. Transform:  
   - Cleans movie titles, extracts the release year, and removes duplicates.  
   - Maps genres to a separate table with proper relationships.

3. Load:  
   - Loads the transformed data into a SQL Server database (`MovieDB`) using `pyodbc`.  
   - Ensures idempotency (no duplicate inserts) using the `MERGE` command in SQL Server.

4. Analyze:  
   - Runs SQL queries to find insights like:
     - Highest-rated movie  
     - Top 5 genres by average rating  
     - Director with most movies  
     - Average rating per release year  

---

## 🧱 Project Structure

```
MoviePipeline/
│
├── etl.py           # Main Python ETL script
├── schema.sql       # Database schema (for SQL Server)
├── queries.sql      # Analytical SQL queries
├── requirements.txt # Python dependencies
├── movies.csv       # Input file (MovieLens dataset)
├── ratings.csv      # Input file (MovieLens dataset)
└── README.md        # Project documentation
```

---

## ⚙️ Setup and Installation

### 1️⃣ Prerequisites
- Python 3.10+
- SQL Server / SSMS
- ODBC Driver 17 or 18 for SQL Server
- VS Code (optional for editing)
- OMDb API key (free at [omdbapi.com/apikey.aspx](https://www.omdbapi.com/apikey.aspx))

---

### 2️⃣ Steps to Set Up Environment

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/MoviePipeline.git
   cd MoviePipeline
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Create a `.env` file in the project folder and add:
   ```
   OMDB_API_KEY=your_api_key_here
   DB_SERVER=YOUR_PC_NAME\MSSQLSERVER01
   DB_NAME=MovieDB
   DB_DRIVER=ODBC Driver 18 for SQL Server
   DB_TRUSTED=yes
   DB_ENCRYPT=yes
   DB_TRUST_CERT=yes
   ```

5. In SQL Server Management Studio (SSMS):
   - Create a new database called `MovieDB`.
   - Run `schema.sql` to create tables.

6. Place `movies.csv` and `ratings.csv` (from MovieLens) in the project folder.

---

### 3️⃣ Run the ETL Pipeline

```bash
python etl.py
```

- This will connect to SQL Server, extract and transform the data, fetch details from the OMDb API, and load everything into the database.

---

### 4️⃣ Run SQL Queries

In SSMS, connect to `MovieDB` and run:
```sql
-- Example
SELECT TOP 1 M.title, AVG(R.rating) AS avg_rating
FROM Movies M
JOIN Ratings R ON M.movie_id = R.movie_id
GROUP BY M.title
ORDER BY avg_rating DESC;
```

Or execute the full `queries.sql` file.

---

## 🧠 Design Choices and Assumptions

- Database: SQL Server was used for relational modeling and strong schema enforcement.  
- ORM: Instead of using ORM, `pyodbc` was chosen for direct SQL execution and better control.  
- Data Idempotency: Implemented using SQL Server `MERGE` to update existing records or insert new ones.  
- API Caching: OMDb responses are stored in a local JSON file (`omdb_cache.json`) to avoid repeated calls.  
- Environment Variables: Used `.env` for secure handling of credentials and configuration.

---

## ⚡ Challenges Faced and Solutions

| Challenge | Solution |
|------------|-----------|
| SQL Server connection SSL error | Fixed by adding `Encrypt=yes;TrustServerCertificate=yes` in connection string |
| OMDb API rate limiting and slowness | Added local cache (`omdb_cache.json`) and small time delay between calls |
| Long ETL runtime | Used caching, bulk inserts (`executemany()`), and reduced commit frequency |
| Schema mismatch errors | Rewrote schema using SQL Server T-SQL syntax (instead of SQLite style) |
| Debugging environment variables | Used `python-dotenv` to manage `.env` configurations properly |

---

## 🔍 Example Analytical Results

| Query | Example Output |
|--------|----------------|
| Highest Rated Movie | The Shawshank Redemption (1994) — 4.5 avg |
| Top 5 Genres | Drama, Action, Crime, Adventure, Comedy |
| Director with Most Movies | Steven Spielberg |
| Avg Rating by Year | Shows gradual improvement trend from 1980s to 2000s |

(Results depend on dataset and available OMDb data)



## 🙌 Acknowledgements
- [MovieLens Dataset – GroupLens](https://grouplens.org/datasets/movielens/latest/)
- [OMDb API](https://www.omdbapi.com/)
- Microsoft SQL Server and ODBC Driver

---

