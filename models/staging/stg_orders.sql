with source as (
    select * from {{ source('jaffle_shop', 'raw_orders') }}
),
renamed as (
    select
        store_id,    
        id as orders_id,
        customer as customer_id,
        ordered_at as order_date
    from source
)
select 
    store_id,
    orders_id,
    customer_id,
    order_date
from renamed


