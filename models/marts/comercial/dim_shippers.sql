with shippers as (
    select *
     FROM {{ ref('stg_erp__shippers')}}),

     transformed as (select
        row_number() over (order by id_transportador ) as sk_transportadora,
        id_transportador,
        nome_fornecedor,
        telefone,
        from shippers
)

select * from shippers
