WITH invoice_items_per_day AS (SELECT * FROM {{ ref('int_ssot_mrr__invoice_items_per_day') }} )

, days_between AS (
SELECT
  *,
  IF( DATE_DIFF(period_end_date, period_start_date, DAY) = 0
      ,1
      , DATE_DIFF(period_end_date, period_start_date, DAY)) AS n_days_between_start_and_end
FROM invoice_items_per_day
WHERE TRUE
)

, flag_mrr AS (
SELECT
  *,
  amount_eur/n_days_between_start_and_end AS amount_per_day_eur,
  IF( type = 'subscription'
      , true
      , false) AS is_mrr
FROM days_between
)

, mrr_amount AS (
SELECT
  *,
  IF( is_mrr
      , amount_per_day_eur
      , 0) AS mrr_amount_eur
FROM flag_mrr
)


SELECT
  *
FROM mrr_amount