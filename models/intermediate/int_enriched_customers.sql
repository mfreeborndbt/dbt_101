-- Intermediate model joining customers and nations
-- This enriches customer data with nation information

with customers as (
    select * from {{ ref('stg_customer') }}
),

nations as (
    select * from {{ ref('stg_nation') }}
),

joined as (
    select
        -- Customer information
        customers.customer_key,
        customers.customer_name,
        customers.customer_address,
        customers.phone_number,
        customers.account_balance,
        customers.market_segment,
        
        -- Nation information
        customers.nation_key,
        nations.nation_name,
        nations.region_key
        
    from customers
    inner join nations
        on customers.nation_key = nations.nation_key
)

select * from joined

