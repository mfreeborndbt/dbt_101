with nation_revenue as (
    select
        nation_name,
        sum(total_revenue_final) as total_revenue_final
    from {{ ref('customer_orders') }}
    group by nation_name
)

select * from nation_revenue