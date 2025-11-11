-- Marts model aggregating customer order metrics
-- This model combines enriched customer data with order line items
-- to provide key metrics at the customer level

with enriched_customers as (
    select * from {{ ref('int_enriched_customers') }}
),

orders_line_items as (
    select * from {{ ref('int_orders_line_items') }}
),

customer_metrics as (
    select
        orders_line_items.customer_key,
        
        -- Order metrics
        count(distinct orders_line_items.order_key) as total_orders,
        count(*) as total_line_items,
        
        -- Date metrics
        min(orders_line_items.order_date) as first_order_date,
        max(orders_line_items.order_date) as most_recent_order_date,
        
        -- Revenue metrics
        sum(orders_line_items.extended_price) as total_revenue,
        sum(orders_line_items.discounted_price) as total_revenue_after_discount,
        sum(orders_line_items.final_price) as total_revenue_final,
        avg(orders_line_items.final_price) as avg_line_item_revenue,
        
        -- Discount metrics
        avg(orders_line_items.discount) as avg_discount_rate,
        sum(orders_line_items.extended_price - orders_line_items.discounted_price) as total_discount_amount,
        
        -- Quantity metrics
        sum(orders_line_items.quantity) as total_quantity_ordered
        
    from orders_line_items
    group by orders_line_items.customer_key
),

final as (
    select
        -- Customer information
        enriched_customers.customer_key,
        enriched_customers.customer_name,
        enriched_customers.nation_name,
        enriched_customers.region_key,
        enriched_customers.market_segment,
        enriched_customers.account_balance,
        
        -- Order metrics
        customer_metrics.total_orders,
        customer_metrics.total_line_items,
        customer_metrics.first_order_date,
        customer_metrics.most_recent_order_date,
        
        -- Revenue metrics
        customer_metrics.total_revenue,
        customer_metrics.total_revenue_after_discount,
        customer_metrics.total_revenue_final,
        customer_metrics.avg_line_item_revenue,
        
        -- Discount metrics
        customer_metrics.avg_discount_rate,
        customer_metrics.total_discount_amount,
        
        -- Quantity metrics
        customer_metrics.total_quantity_ordered,
        
        -- Calculated metrics
        customer_metrics.total_revenue_final / customer_metrics.total_orders as avg_order_value,
        datediff(day, customer_metrics.first_order_date, customer_metrics.most_recent_order_date) as customer_lifetime_days
        
    from enriched_customers
    inner join customer_metrics
        on enriched_customers.customer_key = customer_metrics.customer_key
)

select * from final

