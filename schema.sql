USE faiyazalam;
GO

CREATE TABLE Movies (
    movie_id INT PRIMARY KEY,
    title NVARCHAR(255) NOT NULL,
    year INT,
    imdb_id NVARCHAR(20) UNIQUE,
    director NVARCHAR(255),
    plot NVARCHAR(MAX),
    box_office NVARCHAR(50),
    runtime_minutes INT,
    language NVARCHAR(255),
    country NVARCHAR(255),
    last_updated DATETIME DEFAULT GETDATE()
);

CREATE TABLE Genres (
    genre_id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE MovieGenres (
    movie_id INT FOREIGN KEY REFERENCES Movies(movie_id),
    genre_id INT FOREIGN KEY REFERENCES Genres(genre_id),
    PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE Ratings (
    id INT IDENTITY(1,1) PRIMARY KEY,
    user_id INT,
    movie_id INT FOREIGN KEY REFERENCES Movies(movie_id),
    rating FLOAT,
    timestamp BIGINT
);
