WITH base AS (SELECT * FROM {{ source('ae_challenge_data','invoice') }} )

SELECT  
    string_field_0 AS invoice_id, 
    string_field_1 AS customer_id
FROM base