{% set sql_store_codes %}
    select
        competitor_id,
        left(zaff_ref, 4) as center,
        right(zaff_ref, 2) as channel
    from {{ ref('stg_stores') }}
    where zaff_ref is not null
    and is_active='S'
{% endset %}

{%- set store_codes = run_query(sql_store_codes) -%}

with internal_prices as (
    
    select * from {{ ref('stg_internal_prices') }}

),
product_map as (

    select * from {{ ref('stg_product_map') }}

),
final as (

    {% for id, center, channel in store_codes %}
        
        SELECT TOP 1 WITH TIES
            
            CONVERT(DATE, internal_prices.extraction_date) AS extraction_date,
            internal_prices.product_id,
            '{{id}}' AS competitor_id,
            internal_prices.price
            
        FROM internal_prices

        INNER JOIN product_map 
            ON product_map.product_id = internal_prices.product_id

        WHERE 
            internal_prices.center IN ('{{center}}',' ') AND 
            internal_prices.channel = '{{channel}}' AND
            internal_prices.extraction_date >= DATEADD(DAY, -34, GETDATE())

        ORDER BY ROW_NUMBER() OVER (
            PARTITION BY 
                internal_prices.product_id, 
                CONVERT(DATE, internal_prices.extraction_date) 
            ORDER BY 
                internal_prices.extraction_date DESC, 
                internal_prices.center DESC, 
                internal_prices.source, 
                internal_prices.price
            )
    
        {% if not loop.last %} union all {% endif %}
    
    {% endfor %}
)
select * from final
