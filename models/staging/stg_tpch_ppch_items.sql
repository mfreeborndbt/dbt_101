-- Orders with line items combined
with orders as (
    select * from {{ source('TPCH', 'ORDERS') }}
),

lineitems as (
    select * from {{ source('TPCH', 'LINEITEM') }}
),

joined as (
    select
        -- Order fields
        orders.o_orderkey as order_key,
        orders.o_custkey as customer_key,
        orders.o_orderstatus as order_status,
        orders.o_totalprice as order_total_price,
        orders.o_orderdate as order_date,
        orders.o_orderpriority as order_priority,
        orders.o_clerk as clerk,
        orders.o_shippriority as ship_priority,
        orders.o_comment as order_comment,
        
        -- Line item fields
        lineitems.l_linenumber as line_number,
        lineitems.l_partkey as part_key,
        lineitems.l_suppkey as supplier_key,
        lineitems.l_quantity as quantity,
        lineitems.l_extendedprice as extended_price,
        lineitems.l_discount as discount,
        lineitems.l_tax as tax,
        lineitems.l_returnflag as return_flag,
        lineitems.l_linestatus as line_status,
        lineitems.l_shipdate as ship_date,
        lineitems.l_commitdate as commit_date,
        lineitems.l_receiptdate as receipt_date,
        lineitems.l_shipinstruct as ship_instructions,
        lineitems.l_shipmode as ship_mode,
        lineitems.l_comment as line_comment
        
    from orders
    inner join lineitems
        on orders.o_orderkey = lineitems.l_orderkey
)

select * from joined

