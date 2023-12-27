SELECT CODIGO_DO_PRODUTO, SUM(PRECO) AS PRECO, SUM(QUANTIDADE) AS QTD FROM `curso-big-query-9110.sucos_vendas.itens_notas_fiscais`
GROUP BY 1
