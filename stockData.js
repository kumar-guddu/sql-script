const { Client } = require('pg');
const fs = require('fs').promises;
const path = require('path');

// Database connection variables
const dbConfig = {
  user: 'postgres',  // replace with your RDS username
  host: 'db-insighthread.cpeikys6cqzu.ap-south-1.rds.amazonaws.com',  // replace with your RDS endpoint
  database: 'postgres',  // replace with your database name
  password: 'aitoxradmin',  // replace with your RDS password
  port: 5432,
  ssl: {
    rejectUnauthorized: false,  // this will allow self-signed certificates
  },
};

// Fetch data from JSON file
async function fetchData() {
  const filePath = path.join(__dirname, 'stock_monthly.json');
  try {
    const fileData = await fs.readFile(filePath, 'utf8');
    return JSON.parse(fileData);
  } catch (error) {
    console.error('Error reading data from JSON file:', error);
    return null;
  }
}

// Insert data into PostgreSQL
async function insertData(dbConfig, data) {
  const client = new Client(dbConfig);
  await client.connect();

  const companySymbol = data['Meta Data']['2. Symbol'];
  const dailyData = data['Monthly Time Series'];
  // const dailyData = data['Weekly Time Series'];
  // const dailyData = data['Time Series (Daily)'];

  // Get dates and sort them in descending order
  const dates = Object.keys(dailyData).sort((a, b) => new Date(b) - new Date(a));

  for (const date of dates) {
    const values = dailyData[date];
    const intervalType = 'monthly';
    const open = values['1. open'];
    const high = values['2. high'];
    const low = values['3. low'];
    const close = values['4. close'];
    const volume = values['5. volume'];

    const query = `
      INSERT INTO stock_data (symbol, date, interval_type, open, high, low, close, volume)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      ON CONFLICT (symbol, date, interval_type) DO NOTHING
    `;
    const valuesArray = [companySymbol, date, intervalType, open, high, low, close, volume];

    try {
      await client.query(query, valuesArray);
    } catch (error) {
      console.error('Error inserting data into PostgreSQL:', error);
    }
  }

  await client.end();
}

// Delete all rows from the table
async function deleteAllRows(dbConfig) {
  const client = new Client(dbConfig);
  await client.connect();

  try {
    await client.query('TRUNCATE TABLE stock_data;');
    console.log('All rows deleted successfully.');
  } catch (error) {
    console.error('Error deleting rows from PostgreSQL:', error);
  }

  await client.end();
}

// Main function
async function main() {
  // await deleteAllRows(dbConfig);

  const data = await fetchData();
  if (data) {
    await insertData(dbConfig, data);
    console.log('Data inserted successfully.');
  } else {
    console.error('Error fetching data. Please check your JSON file.');
  }
}

main();
