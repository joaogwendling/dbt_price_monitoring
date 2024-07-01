with product_map as (
    select * from  {{ ref('stg_product_map') }}
),
prices as (
    select * from  {{ ref('competitor_prices_daily') }}
),
competitor_prices_full as (
    SELECT TOP 1 WITH TIES
        prices.extraction_date,
        product_map.product_id,
        prices.competitor_id,
        prices.price
    FROM prices
    INNER JOIN product_map
        ON product_map.competitor_product_id = prices.competitor_product_id
        AND product_map.competitor_name = prices.competitor_name
        AND product_map.origin = prices.origin
    ORDER BY ROW_NUMBER() OVER (
        PARTITION BY 
            prices.extraction_date, 
            product_map.product_id, 
            prices.competitor_id 
        ORDER BY 
            prices.price 
        )
),
final as (
    select * from competitor_prices_full
)

select * from final