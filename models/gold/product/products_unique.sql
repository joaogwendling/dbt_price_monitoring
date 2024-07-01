WITH products AS (

    SELECT * FROM {{ ref('stg_products') }}

),
final AS (
    
    SELECT TOP 1 WITH TIES
        CAST(product_id AS varchar(10)) AS product_id,
        product_name,
        product_status,
        product_area,
        product_department,
        product_class,
        product_group
    FROM products
    WHERE product_id > 1
    ORDER BY ROW_NUMBER() OVER(
        PARTITION BY 
            product_id 
        ORDER BY 
            extraction_id DESC, 
            extraction_date DESC,
            IIF(product_status IN ('BC','NO','LP'),0,1)
        )

)

SELECT * FROM final
