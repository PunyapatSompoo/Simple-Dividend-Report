SELECT
  Ticker,
  EXTRACT(year FROM Date) AS year,
  ROUND(SUM(Dividends), 2) As dividend_per_year
FROM 
  `cal-dividends.thai_tickers.price_and_dividends`
WHERE
  Dividends != 0
GROUP BY
  Ticker, year
ORDER BY
  Ticker
