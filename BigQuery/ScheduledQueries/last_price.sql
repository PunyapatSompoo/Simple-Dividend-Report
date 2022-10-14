SELECT
  B.Ticker,
  EXTRACT(year FROM B.Date) AS year,
  ROUND(B.Price, 2) AS last_price,
FROM (
      SELECT Ticker, MAX(Date) as MaxDate
      FROM `cal-dividends.thai_tickers.price_and_dividends`
      GROUP BY Ticker
) A
INNER JOIN `cal-dividends.thai_tickers.price_and_dividends` B
ON B.Ticker = A.Ticker AND B.Date = A.MaxDate
ORDER BY Ticker
