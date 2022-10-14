SELECT
  A.Ticker,
  A.year,
  A.dividend_per_year,
  B.last_price
FROM (
      SELECT
        Ticker,
        year,
        dividend_per_year
  FROM
    `cal-dividends.thai_tickers.dividend_per_year`
) A
LEFT JOIN (
          SELECT
            Ticker,
            year,
            last_price
          FROM
            `cal-dividends.thai_tickers.last_price`
) B 
ON A.Ticker = B.Ticker
ORDER BY Ticker, year
