WITH source AS (
    select * from {{ source('db', 'dados_raspagem') }}
),
renamed AS (
    select
        data_extracao as extraction_date_full,
        nome_concorrente as competitor_full_name,
        id_prod_concorrente as competitor_product_id,
        nome_prod_concorrente as competitor_product_name,
        preco as price
    from source
)

SELECT * FROM renamed

