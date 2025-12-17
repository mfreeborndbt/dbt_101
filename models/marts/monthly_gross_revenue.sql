-- Monthly gross revenue aggregation for reporting
with fct_order_items as (
    select * from {{ ref('fct_order_items') }}
),

monthly_aggregated as (
    select
        date_trunc('month', order_date) as order_month,
        
        -- Geographic dimensions
        region_name,
        nation_name,
        market_segment,
        
        -- Aggregated metrics
        count(distinct order_key) as total_orders,
        count(distinct customer_key) as total_customers,
        count(*) as total_line_items,
        
        -- Revenue metrics
        sum(extended_price) as gross_revenue,
        sum(discounted_price) as discounted_revenue,
        sum(net_item_sales) as net_sales,
        sum(total_supply_cost) as total_supply_cost,
        sum(gross_profit) as gross_profit,
        
        -- Average metrics
        avg(discount) as avg_discount_rate,
        avg(quantity) as avg_quantity,
        avg(net_item_sales) as avg_line_item_sales,
        
        -- Margin calculation
        sum(gross_profit) / nullif(sum(net_item_sales), 0) as gross_margin
        
    from fct_order_items
    group by 
        date_trunc('month', order_date),
        region_name,
        nation_name,
        market_segment
)

select * from monthly_aggregated

