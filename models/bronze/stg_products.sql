WITH source AS (
    select * from {{ source('db', 'sap_arvore_mercadologica') }} 
),
renamed AS (
    select
        id_monitoramento as extraction_id,
        data_extração as extraction_date,
        cod_material_sap as product_id,
        descricao_material_sap as product_name,
        sm as product_status,
        area as product_area,
        departamento as product_department,
        secao as product_session,
        classe as product_class,
        grupo as product_group
    from source
)

SELECT * FROM renamed