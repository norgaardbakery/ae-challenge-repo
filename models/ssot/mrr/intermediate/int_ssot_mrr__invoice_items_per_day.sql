WITH invoice_items AS (SELECT * FROM {{ ref('int_ssot_mrr__invoice_items') }} )

, company_start_to_end_month AS (
SELECT
    customer_id,
    MIN(DATE_TRUNC(period_start_date, MONTH)) AS min_month,
    MAX(DATE_TRUNC(period_end_date, MONTH)) AS max_month
FROM invoice_items
GROUP BY 1
)

, company_end_date_formatting AS (
SELECT 
    * EXCEPT(max_month),
    DATE_ADD(LAST_DAY(max_month), INTERVAL 1 MONTH) AS max_month
FROM company_start_to_end_month
)

, company_day_spine AS (
SELECT 
  *,
FROM company_end_date_formatting,
UNNEST(GENERATE_DATE_ARRAY(
        min_month
        , max_month
        , INTERVAL 1 DAY)
      ) AS period_day
)

, invoice_item_day_spine AS (
SELECT 
  *,
FROM invoice_items,
UNNEST(GENERATE_DATE_ARRAY(
        period_start_date
        , period_end_date
        , INTERVAL 1 DAY)
      ) AS period_day
)

SELECT
    * EXCEPT(amount, amount_eur),
    IFNULL(i.amount, 0) AS amount,
    IFNULL(amount_eur,0) AS amount_eur
FROM company_day_spine c
LEFT JOIN invoice_item_day_spine i USING (customer_id, period_day)