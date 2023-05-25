WITH base AS (SELECT * FROM {{ source('ae_challenge_data','invoice_item') }} )

SELECT  
    id AS invoice_item_id,
    invoice_id,
    period_start AS period_start_at,
    period_end AS period_end_at,
    DATE(period_start) AS period_start_date,
    DATE(period_end) AS period_end_date,
    type,
    amount,
    currency,
    _synced AS synced_at
FROM base