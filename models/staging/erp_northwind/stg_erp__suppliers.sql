with source_suppliers as (
    select 
        cast(supplier_id as int64) as id_fornecedor					
        , cast(company_name as string)	as nome_fornecedor				
        , cast(contact_name as string)	as nome_contato_fornecedor 				
        , cast(contact_title as string)	as titulo_contato_fornecedor				
        , cast(address as string)	as endereco_fornecedor 				
        , cast(city as string)	as cidade_fornecedor 				
        , cast(region as string)	as 	regiao_fornecedor			
        , cast(postal_code as string)	as 	codigo_postal_fornecedor			
        , cast(country as string)	as pais_fornecedor 				
        , cast(phone as string)	as telefone_fornecedor	
        , cast(fax as string)	as fax_fornecedor 				
        --, homepage			
    from {{ source('erp','suppliers')}}
)
select * from source_suppliers