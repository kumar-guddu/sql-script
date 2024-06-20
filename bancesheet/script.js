const { Client } = require('pg');
const fs = require('fs').promises;

const client = new Client({
  user: 'postgres', // replace with your RDS username
  host: 'db-insighthread.cpeikys6cqzu.ap-south-1.rds.amazonaws.com', // replace with your RDS endpoint
  database: 'postgres', // replace with your database name
  password: 'aitoxradmin', // replace with your RDS password
  port: 5432,
  ssl: {
    rejectUnauthorized: false
  }
});

function toNumber(value) {
  return value === 'None' ? null : Number(value);
}

function toDate(value) {
  return value === 'None' ? null : new Date(value);
}

async function insertBalanceSheetAnnual(data) {
  const query = `
    INSERT INTO balance_sheet_annual (
      symbol, fiscal_date_ending, reported_currency, total_assets, total_current_assets,
      cash_and_cash_equivalents_at_carrying_value, cash_and_short_term_investments,
      inventory, current_net_receivables, total_non_current_assets,
      property_plant_equipment, accumulated_depreciation_amortization_ppe, intangible_assets,
      intangible_assets_excluding_goodwill, goodwill, investments, long_term_investments,
      short_term_investments, other_current_assets, other_non_current_assets, total_liabilities,
      total_current_liabilities, current_accounts_payable, deferred_revenue, current_debt,
      short_term_debt, total_non_current_liabilities, capital_lease_obligations, long_term_debt,
      current_long_term_debt, long_term_debt_noncurrent, short_long_term_debt_total,
      other_current_liabilities, other_non_current_liabilities, total_shareholder_equity,
      treasury_stock, retained_earnings, common_stock, common_stock_shares_outstanding
    ) VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21,
      $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39
    ) ON CONFLICT (symbol, fiscal_date_ending) DO NOTHING
  `;

  const values = [
    data.symbol, toDate(data.fiscalDateEnding), data.reportedCurrency, toNumber(data.totalAssets), 
    toNumber(data.totalCurrentAssets), toNumber(data.cashAndCashEquivalentsAtCarryingValue), 
    toNumber(data.cashAndShortTermInvestments), toNumber(data.inventory), toNumber(data.currentNetReceivables), 
    toNumber(data.totalNonCurrentAssets), toNumber(data.propertyPlantEquipment), 
    toNumber(data.accumulatedDepreciationAmortizationPPE), toNumber(data.intangibleAssets), 
    toNumber(data.intangibleAssetsExcludingGoodwill), toNumber(data.goodwill), toNumber(data.investments), 
    toNumber(data.longTermInvestments), toNumber(data.shortTermInvestments), toNumber(data.otherCurrentAssets), 
    toNumber(data.otherNonCurrentAssets), toNumber(data.totalLiabilities), toNumber(data.totalCurrentLiabilities), 
    toNumber(data.currentAccountsPayable), toNumber(data.deferredRevenue), toNumber(data.currentDebt), 
    toNumber(data.shortTermDebt), toNumber(data.totalNonCurrentLiabilities), 
    toNumber(data.capitalLeaseObligations), toNumber(data.longTermDebt), toNumber(data.currentLongTermDebt), 
    toNumber(data.longTermDebtNoncurrent), toNumber(data.shortLongTermDebtTotal), toNumber(data.otherCurrentLiabilities), 
    toNumber(data.otherNonCurrentLiabilities), toNumber(data.totalShareholderEquity), toNumber(data.treasuryStock), 
    toNumber(data.retainedEarnings), toNumber(data.commonStock), toNumber(data.commonStockSharesOutstanding)
  ];

  try {
    await client.query(query, values);
    console.log(`Inserted annual balance sheet report for ${data.symbol} (${data.fiscalDateEnding})`);
  } catch (err) {
    console.error(`Error inserting annual balance sheet report for ${data.symbol} (${data.fiscalDateEnding}):`, err);
  }
}

async function insertBalanceSheetQuarterly(data) {
  const query = `
    INSERT INTO balance_sheet_quarterly (
      symbol, fiscal_date_ending, reported_currency, total_assets, total_current_assets,
      cash_and_cash_equivalents_at_carrying_value, cash_and_short_term_investments,
      inventory, current_net_receivables, total_non_current_assets,
      property_plant_equipment, accumulated_depreciation_amortization_ppe, intangible_assets,
      intangible_assets_excluding_goodwill, goodwill, investments, long_term_investments,
      short_term_investments, other_current_assets, other_non_current_assets, total_liabilities,
      total_current_liabilities, current_accounts_payable, deferred_revenue, current_debt,
      short_term_debt, total_non_current_liabilities, capital_lease_obligations, long_term_debt,
      current_long_term_debt, long_term_debt_noncurrent, short_long_term_debt_total,
      other_current_liabilities, other_non_current_liabilities, total_shareholder_equity,
      treasury_stock, retained_earnings, common_stock, common_stock_shares_outstanding
    ) VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21,
      $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39
    ) ON CONFLICT (symbol, fiscal_date_ending) DO NOTHING
  `;

  const values = [
    data.symbol, toDate(data.fiscalDateEnding), data.reportedCurrency, toNumber(data.totalAssets), 
    toNumber(data.totalCurrentAssets), toNumber(data.cashAndCashEquivalentsAtCarryingValue), 
    toNumber(data.cashAndShortTermInvestments), toNumber(data.inventory), toNumber(data.currentNetReceivables), 
    toNumber(data.totalNonCurrentAssets), toNumber(data.propertyPlantEquipment), 
    toNumber(data.accumulatedDepreciationAmortizationPPE), toNumber(data.intangibleAssets), 
    toNumber(data.intangibleAssetsExcludingGoodwill), toNumber(data.goodwill), toNumber(data.investments), 
    toNumber(data.longTermInvestments), toNumber(data.shortTermInvestments), toNumber(data.otherCurrentAssets), 
    toNumber(data.otherNonCurrentAssets), toNumber(data.totalLiabilities), toNumber(data.totalCurrentLiabilities), 
    toNumber(data.currentAccountsPayable), toNumber(data.deferredRevenue), toNumber(data.currentDebt), 
    toNumber(data.shortTermDebt), toNumber(data.totalNonCurrentLiabilities), 
    toNumber(data.capitalLeaseObligations), toNumber(data.longTermDebt), toNumber(data.currentLongTermDebt), 
    toNumber(data.longTermDebtNoncurrent), toNumber(data.shortLongTermDebtTotal), toNumber(data.otherCurrentLiabilities), 
    toNumber(data.otherNonCurrentLiabilities), toNumber(data.totalShareholderEquity), toNumber(data.treasuryStock), 
    toNumber(data.retainedEarnings), toNumber(data.commonStock), toNumber(data.commonStockSharesOutstanding)
  ];

  try {
    await client.query(query, values);
    console.log(`Inserted quarterly balance sheet report for ${data.symbol} (${data.fiscalDateEnding})`);
  } catch (err) {
    console.error(`Error inserting quarterly balance sheet report for ${data.symbol} (${data.fiscalDateEnding}):`, err);
  }
}

async function processJsonFile(filePath) {
  try {
    const jsonString = await fs.readFile(filePath, 'utf8');
    const jsonData = JSON.parse(jsonString);

    for (const report of jsonData.annualReports) {
      await insertBalanceSheetAnnual({
        symbol: jsonData.symbol,
        fiscalDateEnding: report.fiscalDateEnding,
        reportedCurrency: report.reportedCurrency,
        totalAssets: report.totalAssets,
        totalCurrentAssets: report.totalCurrentAssets,
        cashAndCashEquivalentsAtCarryingValue: report.cashAndCashEquivalentsAtCarryingValue,
        cashAndShortTermInvestments: report.cashAndShortTermInvestments,
        inventory: report.inventory,
        currentNetReceivables: report.currentNetReceivables,
        totalNonCurrentAssets: report.totalNonCurrentAssets,
        propertyPlantEquipment: report.propertyPlantEquipment,
        accumulatedDepreciationAmortizationPPE: report.accumulatedDepreciationAmortizationPPE,
        intangibleAssets: report.intangibleAssets,
        intangibleAssetsExcludingGoodwill: report.intangibleAssetsExcludingGoodwill,
        goodwill: report.goodwill,
        investments: report.investments,
        longTermInvestments: report.longTermInvestments,
        shortTermInvestments: report.shortTermInvestments,
        otherCurrentAssets: report.otherCurrentAssets,
        otherNonCurrentAssets: report.otherNonCurrentAssets,
        totalLiabilities: report.totalLiabilities,
        totalCurrentLiabilities: report.totalCurrentLiabilities,
        currentAccountsPayable: report.currentAccountsPayable,
        deferredRevenue: report.deferredRevenue,
        currentDebt: report.currentDebt,
        shortTermDebt: report.shortTermDebt,
        totalNonCurrentLiabilities: report.totalNonCurrentLiabilities,
        capitalLeaseObligations: report.capitalLeaseObligations,
        longTermDebt: report.longTermDebt,
        currentLongTermDebt: report.currentLongTermDebt,
        longTermDebtNoncurrent: report.longTermDebtNoncurrent,
        shortLongTermDebtTotal: report.shortLongTermDebtTotal,
        otherCurrentLiabilities: report.otherCurrentLiabilities,
        otherNonCurrentLiabilities: report.otherNonCurrentLiabilities,
        totalShareholderEquity: report.totalShareholderEquity,
        treasuryStock: report.treasuryStock,
        retainedEarnings: report.retainedEarnings,
        commonStock: report.commonStock,
        commonStockSharesOutstanding: report.commonStockSharesOutstanding
      });
    }

    for (const report of jsonData.quarterlyReports) {
      await insertBalanceSheetQuarterly({
        symbol: jsonData.symbol,
        fiscalDateEnding: report.fiscalDateEnding,
        reportedCurrency: report.reportedCurrency,
        totalAssets: report.totalAssets,
        totalCurrentAssets: report.totalCurrentAssets,
        cashAndCashEquivalentsAtCarryingValue: report.cashAndCashEquivalentsAtCarryingValue,
        cashAndShortTermInvestments: report.cashAndShortTermInvestments,
        inventory: report.inventory,
        currentNetReceivables: report.currentNetReceivables,
        totalNonCurrentAssets: report.totalNonCurrentAssets,
        propertyPlantEquipment: report.propertyPlantEquipment,
        accumulatedDepreciationAmortizationPPE: report.accumulatedDepreciationAmortizationPPE,
        intangibleAssets: report.intangibleAssets,
        intangibleAssetsExcludingGoodwill: report.intangibleAssetsExcludingGoodwill,
        goodwill: report.goodwill,
        investments: report.investments,
        longTermInvestments: report.longTermInvestments,
        shortTermInvestments: report.shortTermInvestments,
        otherCurrentAssets: report.otherCurrentAssets,
        otherNonCurrentAssets: report.otherNonCurrentAssets,
        totalLiabilities: report.totalLiabilities,
        totalCurrentLiabilities: report.totalCurrentLiabilities,
        currentAccountsPayable: report.currentAccountsPayable,
        deferredRevenue: report.deferredRevenue,
        currentDebt: report.currentDebt,
        shortTermDebt: report.shortTermDebt,
        totalNonCurrentLiabilities: report.totalNonCurrentLiabilities,
        capitalLeaseObligations: report.capitalLeaseObligations,
        longTermDebt: report.longTermDebt,
        currentLongTermDebt: report.currentLongTermDebt,
        longTermDebtNoncurrent: report.longTermDebtNoncurrent,
        shortLongTermDebtTotal: report.shortLongTermDebtTotal,
        otherCurrentLiabilities: report.otherCurrentLiabilities,
        otherNonCurrentLiabilities: report.otherNonCurrentLiabilities,
        totalShareholderEquity: report.totalShareholderEquity,
        treasuryStock: report.treasuryStock,
        retainedEarnings: report.retainedEarnings,
        commonStock: report.commonStock,
        commonStockSharesOutstanding: report.commonStockSharesOutstanding
      });
    }

  } catch (err) {
    console.error('Error processing JSON file:', err);
  }
}

(async () => {
  await client.connect();
  await processJsonFile('balanceSheet.json');
  await client.end();
})();
