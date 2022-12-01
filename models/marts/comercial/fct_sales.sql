with 
    clientes as (
        select *
        from {{ref('dim_customers')}}
    ),
    funcionarios as (
        select *
        from {{ref('dim_employees')}}
    ),
    produtos as (
        select *except(id_produto)
        from {{ref('dim_products')}}
    ),
    transportadoras as (
        select *
        from {{ref('dim_shippers')}}
    ),
    pedidos_item as (
        select * 
        from {{ref('int_sales__pedido_itens')}}
    ),
    joined as (
        select
            id_pedido
            ,sk_cliente as fk_cliente
            ,sk_transportadora as fk_transportadora
            ,sk_funcionario as fk_funcionario
            ,sk_produto as fk_produto
            ,clientes.*except(sk_cliente)
            ,funcionarios.*except(sk_funcionario, titulo_de_cortesia)
            ,produtos.*except(sk_produto)
            ,pedidos_item.*except(id_pedido, id_cliente, id_funcionario)
        from pedidos_item
        left join clientes  on pedidos_item.id_cliente = clientes.id_cliente
        left join funcionarios  on pedidos_item.id_funcionario = funcionarios.id_funcionario
        left join produtos  on pedidos_item.id_produto = produtos.sk_produto
        left join transportadoras  on pedidos_item.id_transportadora = transportadoras.id_transportador
        ),
    transformacoes as (
        select 
            id_pedido || '-' || fk_produto as sk_vendas,
            *,
            preco_da_unidade * quantidade as total_bruto,
            (1 - desconto) * preco_da_unidade * quantidade as total_liquido
        from joined  )

select * from transformacoes