WITH source AS (
    select * from {{ source('db', 'depara_manual') }} 
),
renamed AS (
    select
        nome_concorrente as competitor_name,
        nome_origem as origin,
        id_prod_concorrente as competitor_product_id,
        id_prod_zaffari as product_id
    from source
)

SELECT * FROM renamed