const { Client } = require('pg');
const fs = require('fs');
const csv = require('csv-parser');

// PostgreSQL connection configuration
const client = new Client({
  user: 'postgres', // replace with your RDS username
  host: 'db-insighthread.cpeikys6cqzu.ap-south-1.rds.amazonaws.com', // replace with your RDS endpoint
  database: 'postgres', // replace with your database name
  password: 'aitoxradmin', // replace with your RDS password
  port: 5432, // default PostgreSQL port
  ssl: {
    rejectUnauthorized: false, // this will allow self-signed certificates
    ca: fs.readFileSync('rds-combined-ca-bundle.pem').toString() // path to AWS RDS CA bundle
  }
});

// Connect to the PostgreSQL database
client.connect(err => {
  if (err) {
    console.error('Connection error', err.stack);
  } else {
    console.log('Connected to PostgreSQL database');
  }
});

// Function to insert a single row into the database
const insertData = async (row) => {
  const query = `
    INSERT INTO companies (symbol, name, exchange)
    VALUES ($1, $2, $3)
    ON CONFLICT (symbol) DO NOTHING;
  `;

  const values = [row.symbol, row.name, row.exchange];
  try {
    await client.query(query, values);
    console.log(`Data for ${row.symbol} successfully inserted`);
  } catch (err) {
    console.error(`Error inserting data for ${row.symbol}`, err);
  }
};

// Function to insert all rows from the results array, excluding the first one
const insertAllData = async (data) => {
  for (let i = 1; i < data.length; i++) {
    await insertData(data[i]);
  }
  client.end();
};

// Read and parse the CSV file
const results = [];
fs.createReadStream('data.csv')
  .pipe(csv())
  .on('data', (data) => results.push(data))
  .on('end', async () => {
    // Console log the parsed data
    // console.log(results);

    // Insert all data except the first object
    if (results.length > 1) {
      await insertAllData(results);
    }
  });
