--1st answer
-- Include all movies that share the highest average rating, not just one.
WITH MovieAvg AS (
    SELECT
        M.movie_id,
        M.title,
        AVG(R.rating) AS avg_rating,
        COUNT(R.rating) AS total_ratings
    FROM Movies M
    JOIN Ratings R ON M.movie_id = R.movie_id
    GROUP BY M.movie_id, M.title
)
SELECT *
FROM MovieAvg
WHERE avg_rating = (SELECT MAX(avg_rating) FROM MovieAvg)
ORDER BY title;


--otherwise return only first value order by title
SELECT TOP 1 
    M.title, 
    AVG(R.rating) AS avg_rating, 
    COUNT(*) AS total_ratings
FROM Movies M
JOIN Ratings R ON M.movie_id = R.movie_id
GROUP BY M.title
ORDER BY avg_rating DESC;






--2nd answer
SELECT TOP 5
    G.name AS genre,
    AVG(R.rating) AS avg_rating,
    COUNT(R.rating) AS total_ratings
FROM Ratings R
JOIN Movies M ON R.movie_id = M.movie_id
JOIN MovieGenres MG ON M.movie_id = MG.movie_id
JOIN Genres G ON MG.genre_id = G.genre_id
GROUP BY G.name
ORDER BY avg_rating DESC;




--3rd answer
SELECT TOP 1 
    director, 
    COUNT(*) AS total_movies
FROM Movies
WHERE director IS NOT NULL AND director != ''
GROUP BY director
ORDER BY total_movies DESC;




--4th answer
SELECT 
    M.year,
    AVG(R.rating) AS avg_rating,
    COUNT(R.rating) AS n_ratings
FROM Movies M
JOIN Ratings R ON R.movie_id = M.movie_id
WHERE M.year IS NOT NULL
GROUP BY M.year
ORDER BY M.year;