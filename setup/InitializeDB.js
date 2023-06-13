const fs = require('fs');
const csvParser = require('csv-parser');
const sqlite3 = require('sqlite3').verbose();

// Create a new SQLite database
const db = new sqlite3.Database('movies.db');

// Delete All records
db.all(`delete from movies`);
db.all(`delete from ratings`);



// Create movies table
db.run(`
  CREATE TABLE IF NOT EXISTS movies (
    tconst TEXT PRIMARY KEY,
    primaryTitle TEXT,
    runtimeMinutes INTEGER,
    genres TEXT
  );
`);

// Create ratings table
db.run(`
  CREATE TABLE IF NOT EXISTS ratings (
    tconst TEXT PRIMARY KEY,
    averageRating REAL,
    numVotes INTEGER
  );
`);

// Parse and insert data from movies.csv
fs.createReadStream('data/movies.csv')
  .pipe(csvParser())
  .on('data', (row) => {
    db.run(
      `INSERT INTO movies (tconst, primaryTitle, runtimeMinutes, genres) VALUES (?, ?, ?, ?)`,
      [row.tconst, row.primaryTitle, row.runtimeMinutes, row.genres]
    );
  })
  .on('end', () => {
    console.log('Data inserted into movies table');
  });

// Parse and insert data from ratings.csv
fs.createReadStream('data/ratings.csv')
  .pipe(csvParser())
  .on('data', (row) => {
    db.run(
      `INSERT INTO ratings (tconst, averageRating, numVotes) VALUES (?, ?, ?)`,
      [row.tconst, row.averageRating, row.numVotes]
    );
  })
  .on('end', () => {
    console.log('Data inserted into ratings table');
  });
  

// Close the database connection
db.close();
