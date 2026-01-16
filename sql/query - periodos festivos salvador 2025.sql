SELECT  
	es.Estado_id,
    es.Estado_sigla,
    mun.Municipio,
    ende.Bairro,
    pre.Produto,
    pre.Valor_venda,
    pre.Status_preco,
    pre.Unidade_medida,
    rev.Revenda,
    ende.CEP,
    pre.Data_coleta,
    CASE
		WHEN MONTH(pre.Data_coleta) = 6
			THEN 'Festas Juninas'
        WHEN MONTH(pre.Data_coleta) IN (1, 12)
			THEN 'Festa de Fim de Ano'
    END AS Periodo_festivo        
FROM estado AS es
LEFT JOIN municipio AS mun
	ON es.Estado_id = mun.Estado_id
LEFT JOIN revendas AS rev
	ON mun.Municipio_id = rev.Municipio_id
LEFT JOIN precos AS pre 
	ON rev.Revenda_id = pre.Revenda_id
LEFT JOIN endereco AS ende
		ON mun.Municipio_id = ende.Municipio_id
WHERE es.Estado_sigla = "BA" AND mun.Municipio = "Salvador" AND MONTH(pre.Data_coleta) IN (1, 6, 12)
ORDER BY RAND()
LIMIT 100