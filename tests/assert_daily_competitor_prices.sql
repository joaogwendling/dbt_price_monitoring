with competitor_prices as (
    select * from {{ ref('competitor_prices_daily') }}
)

select
    extraction_date,
    competitor_product_id,
    competitor_id,
    count(*) as count
from competitor_prices
group by
    extraction_date,
    competitor_product_id,
    competitor_id
having count > 1