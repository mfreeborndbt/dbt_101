-- Order items with customer dimension
with ppch_items as (
    select * from {{ ref('stg_tpch_ppch_items') }}
),

customers as (
    select * from {{ ref('int_dim_customers') }}
),

joined as (
    select
        ppch_items.order_key,
        ppch_items.customer_key,
        ppch_items.line_number,
        ppch_items.part_key,
        ppch_items.supplier_key,
        ppch_items.order_status,
        ppch_items.order_total_price,
        ppch_items.order_date,
        ppch_items.order_priority,
        ppch_items.clerk,
        ppch_items.ship_priority,
        ppch_items.quantity,
        ppch_items.extended_price,
        ppch_items.discount,
        ppch_items.tax,
        ppch_items.return_flag,
        ppch_items.line_status,
        ppch_items.ship_date,
        ppch_items.commit_date,
        ppch_items.receipt_date,
        ppch_items.ship_mode,
        
        -- Customer dimension fields
        customers.customer_name,
        customers.nation_key,
        customers.nation_name,
        customers.region_key,
        customers.region_name,
        customers.market_segment,
        
        -- Calculated fields
        ppch_items.extended_price * (1 - ppch_items.discount) as discounted_price,
        ppch_items.extended_price * (1 - ppch_items.discount) * (1 + ppch_items.tax) as net_item_sales
        
    from ppch_items
    inner join customers
        on ppch_items.customer_key = customers.customer_key
)

select * from joined

