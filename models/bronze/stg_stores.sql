WITH source AS (
    select * from {{ source('db', 'ref_concorrentes') }} 
),
renamed AS (
    select
        id_concorrente as competitor_id,
        nome_completo_concorrente as competitor_full_name,
        nome_concorrente as competitor_name,
        nome_unidade as unit,
        nome_origem as origin,
        ativo as is_active,
        sigla as competitor_abbr,
        zaff_ref
    from source
)

SELECT * FROM renamed