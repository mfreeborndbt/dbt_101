-- Fact table for order items with part and supplier information
with order_items as (
    select * from {{ ref('order_items') }}
),

parts as (
    select * from {{ ref('stg_tpch_parts') }}
),

part_suppliers as (
    select * from {{ ref('stg_tpch_part_suppliers') }}
),

joined as (
    select
        order_items.order_key,
        order_items.customer_key,
        order_items.line_number,
        order_items.part_key,
        order_items.supplier_key,
        order_items.order_status,
        order_items.order_date,
        order_items.order_priority,
        order_items.clerk,
        order_items.ship_priority,
        order_items.quantity,
        order_items.extended_price,
        order_items.discount,
        order_items.tax,
        order_items.return_flag,
        order_items.line_status,
        order_items.ship_date,
        order_items.commit_date,
        order_items.receipt_date,
        order_items.ship_mode,
        order_items.discounted_price,
        order_items.net_item_sales,
        
        -- Customer dimension
        order_items.customer_name,
        order_items.nation_key,
        order_items.nation_name,
        order_items.region_key,
        order_items.region_name,
        order_items.market_segment,
        
        -- Part information
        parts.part_name,
        parts.manufacturer,
        parts.brand,
        parts.part_type,
        parts.part_size,
        parts.container,
        parts.retail_price,
        
        -- Supplier information
        part_suppliers.available_quantity,
        part_suppliers.supply_cost,
        
        -- Additional calculated fields
        order_items.quantity * part_suppliers.supply_cost as total_supply_cost,
        order_items.net_item_sales - (order_items.quantity * part_suppliers.supply_cost) as gross_profit
        
    from order_items
    inner join parts
        on order_items.part_key = parts.part_key
    inner join part_suppliers
        on order_items.part_key = part_suppliers.part_key
        and order_items.supplier_key = part_suppliers.supplier_key
)

select * from joined

