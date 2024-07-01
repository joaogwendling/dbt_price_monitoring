with competitor_prices as (

    select * from {{ ref('stg_competitor_prices') }}

),
stores as (

    select * from {{ ref('stg_stores') }}

),
final as (
    select top 1 with ties
        CONVERT(DATE, competitor_prices.extraction_date_full) AS extraction_date,
        CASE WHEN stores.origin='IFOOD' THEN REPLACE(competitor_prices.competitor_product_name, ' ', '') ELSE competitor_prices.competitor_product_id END as competitor_product_id,
        competitor_prices.price,
        stores.competitor_id,
        stores.competitor_name,
        stores.origin

    from competitor_prices
    inner join stores
        ON stores.competitor_full_name = competitor_prices.competitor_full_name 
        AND stores.is_active = 'S'
        
    where
        extraction_date_full > DATEADD(DAY, -34, GETDATE())

    ORDER BY ROW_NUMBER() OVER (
        PARTITION BY 
            CASE WHEN stores.origin='IFOOD' THEN REPLACE(competitor_prices.competitor_product_name, ' ', '') ELSE competitor_prices.competitor_product_id END, 
            CONVERT(DATE, competitor_prices.extraction_date_full), 
            stores.competitor_id 
        ORDER BY competitor_prices.extraction_date_full DESC )
    )
select * from final


