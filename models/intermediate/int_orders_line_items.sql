-- Intermediate model joining orders and line items
-- This creates a comprehensive view of order details at the line item level

with orders as (
    select * from {{ ref('stg_orders') }}
),

line_items as (
    select * from {{ ref('stg_lineitem') }}
),

joined as (
    select
        -- Order information
        orders.order_key,
        orders.customer_key,
        orders.order_status,
        orders.total_price as order_total_price,
        orders.order_date,
        orders.order_priority,
        orders.clerk,
        orders.ship_priority,
        
        -- Line item information
        line_items.line_number,
        line_items.part_key,
        line_items.supplier_key,
        line_items.quantity,
        line_items.extended_price,
        line_items.discount,
        line_items.tax,
        line_items.return_flag,
        line_items.line_status,
        line_items.ship_date,
        line_items.commit_date,
        line_items.receipt_date,
        line_items.ship_mode,
        
        -- Calculated fields
        line_items.extended_price * (1 - line_items.discount) as discounted_price,
        line_items.extended_price * (1 - line_items.discount) * (1 + line_items.tax) as final_price
        
    from orders
    inner join line_items
        on orders.order_key = line_items.order_key
)

select * from joined

