with source_employees as (
    select 
        cast(employee_id as INT) as id_funcionario,				
        cast(reports_to as INT) as id_gerente,				
        cast(first_name as STRING) as nome_do_funcionario,				
        cast(last_name as STRING) as sobrenome_do_funcionario,
        cast((first_name || ' ' || last_name) as string) as nome_completo_funcionario,			
        cast(title as STRING) as cargo,				
        cast(title_of_courtesy as STRING) as titulo_de_cortesia,				
        cast(birth_date as DATE) as data_de_nascimento,				
        cast(hire_date as DATE) as data_de_contratacao,				
        cast(address as STRING) as endereco,				
        cast(city as STRING) as cidade,				
        cast(region as STRING) as regiao,				
        cast(postal_code as STRING) as cep,				
        cast(country as STRING) as pais,				
        cast(home_phone as STRING) as telefone,				
        cast(notes as STRING) as observacoes,				
        --cast(extension as INT) as a,				
        --cast(photo as STRING) as a,				
        --cast(photo_path as STRING) as a,	
    from {{source('erp','employees')}}	
)

select * from source_employees