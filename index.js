const express = require('express');
const sqlite3 = require('sqlite3').verbose();

const app = express();
app.use(express.json());
const port = 4000;

// Create a new SQLite database connection
const db = new sqlite3.Database('movies.db');

// GET /api/v1/longest-duration-movies
app.get('/api/v1/longest-duration-movies', (req, res) => {
  db.all(
    `SELECT tconst, primaryTitle, runtimeMinutes, genres FROM movies ORDER BY runtimeMinutes DESC LIMIT 10`,
    (err, rows) => {
      if (err) {
        res.status(500).json({ error: 'Internal server error' });
      } else {
        res.json(rows);
      }
    }
  );
});

// POST /api/v1/new-movie
app.post('/api/v1/new-movie', (req, res) => {
    db.run(`INSERT INTO movies (tconst, primaryTitle, runtimeMinutes, genres) VALUES (?, ?, ?, ?)`,
      [req.body.tconst, req.body.primaryTitle, req.body.runtimeMinutes, req.body.genres]);
    console.log(`Data inserted.`)
    res.send('success');
    // TODO: Handle Error case
});

// GET /api/v1/top-rated-movies
app.get('/api/v1/top-rated-movies', (req, res) => {
  db.all(
    `SELECT movies.tconst, primaryTitle, genres, averageRating FROM movies JOIN ratings ON movies.tconst = ratings.tconst WHERE averageRating > 6.0 ORDER BY averageRating DESC`,
    (err, rows) => {
      if (err) {
        res.status(500).json({ error: `Internal server error ${err}`});
      } else {
        res.json(rows);
      }
    }
  );
});

// GET /api/v1/genre-movies-with-subtotals
app.get('/api/v1/genre-movies-with-subtotals', (req, res) => {
  db.all(
    // `SELECT genres, primaryTitle, numVotes FROM movies GROUP BY genres, primaryTitle WITH ROLLUP`,
    `SELECT genres, primaryTitle, numVotes FROM movies JOIN ratings ON movies.tconst = ratings.tconst GROUP BY ROLLUP(genres, primaryTitle, numVotes)`,
    (err, rows) => {
      if (err) {
        res.status(500).json({ error: `Internal server error ${err.message}` });
      } else {
        res.json(rows);
      }
    }
  );
});

// POST /api/v1/update-runtime-minutes
app.post('/api/v1/update-runtime-minutes', (req, res) => {
  // Handle incrementing runtimeMinutes using SQL query
  db.run(
    `UPDATE movies SET runtimeMinutes = runtimeMinutes + 
    CASE 
      WHEN genres = 'Documentary' THEN 15
      WHEN genres = 'Animation' THEN 30
      ELSE 45
    END`
  );
  res.send('success');
});

// Start the server
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
