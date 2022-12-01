with
    funcionarios as (
        select *
        from {{ ref('stg_erp__employees') }}
    )
    , self_joined as (
        select
            funcionarios.id_funcionario
            , funcionarios.id_gerente
            , funcionarios.nome_do_funcionario
            , funcionarios.sobrenome_do_funcionario
            , funcionarios.nome_completo_funcionario
            , funcionarios.titulo_de_cortesia
            , funcionarios.data_de_nascimento
            , funcionarios.cargo
            , funcionarios.data_de_contratacao
            , gerentes.nome_completo_funcionario as gerente
            , funcionarios.endereco as endereco_funcionario
            , funcionarios.cep as cep_funcionario
            , funcionarios.cidade as cidade_funcionario
            , funcionarios.regiao as regiao_funcionario
            , funcionarios.pais as pais_funcionario
            , funcionarios.telefone as telefone_funcionario
            , funcionarios.observacoes as observacoes_funcionario
        from funcionarios
        left join funcionarios as gerentes on
            funcionarios.id_gerente = gerentes.id_funcionario
    )
    , transformed as (
        select
            row_number() over (order by id_funcionario) as sk_funcionario
            , *
        from self_joined
    )
select *
from transformed