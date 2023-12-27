SELECT CODIGO_DO_PRODUTO, SUM(PRECO) AS PRECO, SUM(QUANTIDADE) as qtd  FROM `curso-big-query-9110.sucos_vendas.itens_notas_fiscais`
GROUP BY 1
