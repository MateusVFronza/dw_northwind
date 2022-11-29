with source_shippers as (
    SELECT 
        cast(shipper_id as int64) as id_transportador,
        cast(company_name as string) nome_fornecedor,
        cast(phone as string) as telefone,
    FROM {{ source('erp','shippers')}}
)

select * from source_shippers