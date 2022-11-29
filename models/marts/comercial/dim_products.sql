with
    categorias as (
        select *
        from {{ref('stg_erp__categories')}}
    ),
    produtos as (
        select *
        from {{ref('stg_erp__products')}}
    ),
    fornecedores as (
        select * 
        from {{ref('stg_erp__suppliers')}}
    ),
    uniao_tabelas as (
        select
            produtos.id_produto
            , produtos.id_fornecedor	
            , produtos.id_categoria	
            , produtos.nome_produto			
            , produtos.quantidade_por_unidade
            , produtos.preco_unitario
            , produtos.unidades_no_estoque		
            , produtos.unidades_por_ordem			
            , produtos.level_de_pedido		
            , produtos.is_discontinuado
            , categorias.nome_categoria			
            , categorias.descricao
            , fornecedores.nome_fornecedor
            , fornecedores.nome_contato_fornecedor
            , fornecedores.titulo_contato_fornecedor
            , fornecedores.endereco_fornecedor
            , fornecedores.codigo_postal_fornecedor
            , fornecedores.cidade_fornecedor
            , fornecedores.regiao_fornecedor
            , fornecedores.pais_fornecedor
            , fornecedores.fax_fornecedor
            , fornecedores.telefone_fornecedor
        from produtos
        left join categorias on produtos.id_categoria = categorias.id_categoria
        left join fornecedores on produtos.id_fornecedor = fornecedores.id_fornecedor
    )
    , transformacoes as (
        select
            row_number() over (order by id_produto) as sk_produto  --- função para criar chave
            , *
        from uniao_tabelas
    )
select *
from transformacoes