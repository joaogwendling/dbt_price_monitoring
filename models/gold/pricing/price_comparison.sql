with competitor_prices as (
    select
        extraction_date,
        product_id,
        competitor_id,
        price
    from {{ ref('competitor_prices_clean') }}
),
internal_prices as (
    select 
        extraction_date,
        product_id,
        competitor_id,
        price
    from {{ ref('internal_prices_clean') }}
),
final as (
    select * from competitor_prices
    union all
    select * from internal_prices
)
select * from final