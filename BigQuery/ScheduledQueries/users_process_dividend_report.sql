SELECT
  user.Ticker,
  dividend.year,
  dividend.last_price,
  user.Volume,
  dividend.dividend_per_year,
FROM(
SELECT
  Ticker,
  Volume
FROM
  `cal-dividends.thai_tickers.users_port`) user
LEFT JOIN(
SELECT
  Ticker,
  year,
  dividend_per_year,
  last_price
FROM
  `cal-dividends.thai_tickers.dividend_report`) dividend
ON dividend.Ticker = user.Ticker
WHERE user.Volume != 0
ORDER BY user.Ticker
