with 
    pedidos as (
        select *
        from {{ref('stg_erp__orders')}}
    ),
    pedidos_item as (
        select *
        from {{ref('stg_erp__orders_details')}}
    ),
    joined as (
        select pedidos.*, pedidos_item.*
        from pedidos
        left join  pedidos_item on pedidos_item.id_pedido = pedidos.id_order
    )
select * from joined