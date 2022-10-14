SELECT 
  a.Ticker,
  a.year,
  a.last_price,
  a.Volume,
  a.dividend_per_year,
  b.total_cost
FROM (SELECT Ticker, year, last_price, Volume, dividend_per_year FROM `cal-dividends.thai_tickers.users_process_dividend_report`) a
LEFT JOIN (SELECT year, SUM(last_price*Volume) as total_cost FROM `cal-dividends.thai_tickers.users_process_dividend_report` GROUP BY year ) b
ON b.year = a.year
WHERE a.year IS NOT NULL
ORDER BY Ticker, year
