-- Customer dimension with nation and region
with customers as (
    select * from {{ ref('stg_tpch_customers') }}
),

nations as (
    select * from {{ ref('stg_tpch_nations') }}
),

regions as (
    select * from {{ ref('stg_tpch_regions') }}
),

joined as (
    select
        customers.customer_key,
        customers.customer_name,
        customers.customer_address,
        customers.phone_number,
        customers.account_balance,
        customers.market_segment,
        customers.nation_key,
        nations.nation_name,
        nations.region_key,
        regions.region_name
    from customers
    inner join nations
        on customers.nation_key = nations.nation_key
    inner join regions
        on nations.region_key = regions.region_key
)

select * from joined

