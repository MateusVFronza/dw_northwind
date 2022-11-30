with 
    source_customers as (
        select
             cast(customer_id as string) as id_cliente					
            , cast(company_name as string) as nome_do_cliente	
            , cast(contact_name as string) as nome_da_empresa
            , cast(contact_title as string) as titulo_de_cortesia					
            , cast(address as string) as endereco
            , cast(city as string) as cidade	
            , cast(region as string) as regiao				
            , cast(postal_code as string) as cep			
            , cast(country as string) as pais		
            , cast(phone as string) as telefone		
            , cast(fax as string) as fax
        from {{ source('erp','customers')}}
    )

select * from source_customers