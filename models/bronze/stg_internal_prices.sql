WITH source AS (
    select * from {{ source('db', 'sap_precos') }} 
),
renamed AS (
    select
        data_extracao as extraction_date,
        material as product_id,
        canal_distribuicao as channel,
        centro as center,
        montante_de_condicao as price,
        fonte as source
    from source
)

SELECT * FROM renamed