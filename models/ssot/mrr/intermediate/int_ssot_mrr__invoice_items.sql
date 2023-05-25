WITH invoices AS (SELECT * FROM {{ ref('stg_ae_challenge_data__invoice') }} )
, invoice_items AS (SELECT * FROM {{ ref('stg_ae_challenge_data__invoice_item') }})
, exchange_rates AS (SELECT * FROM {{ ref('stg_fixed__eur_exchange_rates') }})

, invoice_with_items AS (
SELECT 
    *,
    ii.amount/fx.rate AS amount_eur
FROM invoices i
LEFT JOIN invoice_items ii USING (invoice_id)
LEFT JOIN exchange_rates fx USING (currency)
)

SELECT
    *
FROM invoice_with_items