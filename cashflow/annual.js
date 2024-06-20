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

// Function to convert string to date
const toDate = (dateStr) => {
  return new Date(dateStr);
};

// Function to convert string to number
const toNumber = (numStr) => {
  return numStr === "None" ? null : parseFloat(numStr);
};

// Function to insert annual cash flow report
const insertAnnualCashFlow = async (symbol, report) => {
  const query = `
    INSERT INTO cash_flow_annual (
      symbol, fiscal_date_ending, reported_currency, operating_cashflow, payments_for_operating_activities, 
      proceeds_from_operating_activities, change_in_operating_liabilities, change_in_operating_assets, 
      depreciation_depletion_and_amortization, capital_expenditures, change_in_receivables, change_in_inventory, 
      profit_loss, cashflow_from_investment, cashflow_from_financing, proceeds_from_repayments_of_short_term_debt, 
      payments_for_repurchase_of_common_stock, payments_for_repurchase_of_equity, 
      payments_for_repurchase_of_preferred_stock, dividend_payout, dividend_payout_common_stock, 
      dividend_payout_preferred_stock, proceeds_from_issuance_of_common_stock, proceeds_from_issuance_of_long_term_debt_and_capital_securities, 
      proceeds_from_issuance_of_preferred_stock, proceeds_from_repurchase_of_equity, 
      proceeds_from_sale_of_treasury_stock, change_in_cash_and_cash_equivalents, change_in_exchange_rate, net_income, created_at
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, 
      $22, $23, $24, $25, $26, $27, $28, $29, $30, now()
    )
    ON CONFLICT (symbol, fiscal_date_ending) DO NOTHING;
  `;

  const values = [
    symbol, toDate(report.fiscalDateEnding), report.reportedCurrency, toNumber(report.operatingCashflow), 
    toNumber(report.paymentsForOperatingActivities), toNumber(report.proceedsFromOperatingActivities), 
    toNumber(report.changeInOperatingLiabilities), toNumber(report.changeInOperatingAssets), 
    toNumber(report.depreciationDepletionAndAmortization), toNumber(report.capitalExpenditures), 
    toNumber(report.changeInReceivables), toNumber(report.changeInInventory), toNumber(report.profitLoss), 
    toNumber(report.cashflowFromInvestment), toNumber(report.cashflowFromFinancing), 
    toNumber(report.proceedsFromRepaymentsOfShortTermDebt), toNumber(report.paymentsForRepurchaseOfCommonStock), 
    toNumber(report.paymentsForRepurchaseOfEquity), toNumber(report.paymentsForRepurchaseOfPreferredStock), 
    toNumber(report.dividendPayout), toNumber(report.dividendPayoutCommonStock), 
    toNumber(report.dividendPayoutPreferredStock), toNumber(report.proceedsFromIssuanceOfCommonStock), 
    // toNumber(report.proceedsFromIssuanceOfLongTermDebt), 
    toNumber(report.proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet), 
    toNumber(report.proceedsFromIssuanceOfPreferredStock), toNumber(report.proceedsFromRepurchaseOfEquity), 
    toNumber(report.proceedsFromSaleOfTreasuryStock), toNumber(report.changeInCashAndCashEquivalents), 
    toNumber(report.changeInExchangeRate), toNumber(report.netIncome)
  ];

  try {
    await client.query(query, values);
    console.log(`Annual cash flow report for ${symbol} (${report.fiscalDateEnding}) inserted successfully`);
  } catch (err) {
    console.error(`Error inserting annual cash flow report for ${symbol} (${report.fiscalDateEnding})`, err);
  }
};

// Function to insert quarterly cash flow report
const insertQuarterlyCashFlow = async (symbol, report) => {
  const query = `
    INSERT INTO cash_flow_quarterly (
      symbol, fiscal_date_ending, reported_currency, operating_cashflow, payments_for_operating_activities, 
      proceeds_from_operating_activities, change_in_operating_liabilities, change_in_operating_assets, 
      depreciation_depletion_and_amortization, capital_expenditures, change_in_receivables, change_in_inventory, 
      profit_loss, cashflow_from_investment, cashflow_from_financing, proceeds_from_repayments_of_short_term_debt, 
      payments_for_repurchase_of_common_stock, payments_for_repurchase_of_equity, 
      payments_for_repurchase_of_preferred_stock, dividend_payout, dividend_payout_common_stock, 
      dividend_payout_preferred_stock, proceeds_from_issuance_of_common_stock, proceeds_from_issuance_of_long_term_debt_and_capital_securities, 
      proceeds_from_issuance_of_preferred_stock, proceeds_from_repurchase_of_equity, 
      proceeds_from_sale_of_treasury_stock, change_in_cash_and_cash_equivalents, change_in_exchange_rate, net_income, created_at
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, 
      $22, $23, $24, $25, $26, $27, $28, $29, $30, now()
    )
    ON CONFLICT (symbol, fiscal_date_ending) DO NOTHING;
  `;

  const values = [
    symbol, toDate(report.fiscalDateEnding), report.reportedCurrency, toNumber(report.operatingCashflow), 
    toNumber(report.paymentsForOperatingActivities), toNumber(report.proceedsFromOperatingActivities), 
    toNumber(report.changeInOperatingLiabilities), toNumber(report.changeInOperatingAssets), 
    toNumber(report.depreciationDepletionAndAmortization), toNumber(report.capitalExpenditures), 
    toNumber(report.changeInReceivables), toNumber(report.changeInInventory), toNumber(report.profitLoss), 
    toNumber(report.cashflowFromInvestment), toNumber(report.cashflowFromFinancing), 
    toNumber(report.proceedsFromRepaymentsOfShortTermDebt), toNumber(report.paymentsForRepurchaseOfCommonStock), 
    toNumber(report.paymentsForRepurchaseOfEquity), toNumber(report.paymentsForRepurchaseOfPreferredStock), 
    toNumber(report.dividendPayout), toNumber(report.dividendPayoutCommonStock), 
    toNumber(report.dividendPayoutPreferredStock), toNumber(report.proceedsFromIssuanceOfCommonStock), 
    // toNumber(report.proceedsFromIssuanceOfLongTermDebt), 
    toNumber(report.proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet), 
    toNumber(report.proceedsFromIssuanceOfPreferredStock), toNumber(report.proceedsFromRepurchaseOfEquity), 
    toNumber(report.proceedsFromSaleOfTreasuryStock), toNumber(report.changeInCashAndCashEquivalents), 
    toNumber(report.changeInExchangeRate), toNumber(report.netIncome)
  ];

  try {
    await client.query(query, values);
    console.log(`Quarterly cash flow report for ${symbol} (${report.fiscalDateEnding}) inserted successfully`);
  } catch (err) {
    console.error(`Error inserting quarterly cash flow report for ${symbol} (${report.fiscalDateEnding})`, err);
  }
};

// Function to process the JSON file and insert data
const processJsonFile = async () => {
  const jsonData = fs.readFileSync('cashflow.json');
  const data = JSON.parse(jsonData);

  const symbol = data.symbol;
  const annualReports = data.annualReports;
  const quarterlyReports = data.quarterlyReports;

  for (const report of annualReports) {
    await insertAnnualCashFlow(symbol, report);
  }

  for (const report of quarterlyReports) {
    await insertQuarterlyCashFlow(symbol, report);
  }

  client.end();
};

// Process the JSON file and insert data
processJsonFile().catch(err => console.error('Error processing JSON file', err));
