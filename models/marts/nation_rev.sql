with revenue_by_segment_and_nation as (
    select
        market_segment,
        nation_name,
        sum(total_revenue_final) as total_revenue
    from {{ ref('customer_orders') }}
    group by market_segment, nation_name
    order by market_segment, nation_name
)

select * from revenue_by_segment_and_nation