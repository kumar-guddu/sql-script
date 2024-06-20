const { Client } = require('pg');
const fs = require('fs');

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

// Function to insert a single annual report into the database
const insertQuarterReport = async (report, symbol) => {
  const query = `
    INSERT INTO income_statement_quarterly (
      fiscal_date_ending,
      reported_currency,
      gross_profit,
      total_revenue,
      cost_of_revenue,
      cost_of_goods_and_services_sold,
      operating_income,
      selling_general_and_administrative,
      research_and_development,
      operating_expenses,
      investment_income_net,
      net_interest_income,
      interest_income,
      interest_expense,
      non_interest_income,
      other_non_operating_income,
      depreciation,
      depreciation_and_amortization,
      income_before_tax,
      income_tax_expense,
      interest_and_debt_expense,
      net_income_from_continuing_operations,
      comprehensive_income_net_of_tax,
      ebit,
      ebitda,
      net_income,
      symbol
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
      $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27
    )
    ON CONFLICT (fiscal_date_ending, symbol) DO NOTHING;
  `;

  const values = [
    report.fiscalDateEnding, // Already a string in ISO format
    report.reportedCurrency,
    Number(report.grossProfit),
    Number(report.totalRevenue),
    Number(report.costOfRevenue),
    Number(report.costofGoodsAndServicesSold),
    Number(report.operatingIncome),
    Number(report.sellingGeneralAndAdministrative),
    Number(report.researchAndDevelopment),
    Number(report.operatingExpenses),
    report.investmentIncomeNet === "None" ? null : Number(report.investmentIncomeNet),
    Number(report.netInterestIncome),
    Number(report.interestIncome),
    Number(report.interestExpense),
    report.nonInterestIncome === "None" ? null : Number(report.nonInterestIncome),
    report.otherNonOperatingIncome === "None" ? null : Number(report.otherNonOperatingIncome),
    Number(report.depreciation),
    Number(report.depreciationAndAmortization),
    Number(report.incomeBeforeTax),
    Number(report.incomeTaxExpense),
    Number(report.interestAndDebtExpense),
    Number(report.netIncomeFromContinuingOperations),
    Number(report.comprehensiveIncomeNetOfTax),
    Number(report.ebit),
    Number(report.ebitda),
    Number(report.netIncome),
    symbol
  ];

  try {
    await client.query(query, values);
    console.log(`Quarterly report for ${symbol} (${report.fiscalDateEnding}) successfully inserted`);
  } catch (err) {
    console.error(`Error inserting quearterly report for ${symbol} (${report.fiscalDateEnding})`, err);
  }
};

// Read and parse the JSON file
fs.readFile('income.json', 'utf8', (err, data) => {
  if (err) {
    console.error('Error reading JSON file', err);
    return;
  }

  const jsonData = JSON.parse(data);
  const symbol = jsonData.symbol;
  const quarterlyReports = jsonData.quarterlyReports;
  // console.log(quarterlyReports);

  // Insert each annual report into the database
  (async () => {
    for (const report of quarterlyReports) {
      await insertQuarterReport(report, symbol);
    }

    // Close the database connection
    client.end();
  })();
});
