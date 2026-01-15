SELECT 
	es.Estado_id,
    es.Estado_sigla,
    mun.Municipio,
    ende.Bairro,
    pre.Produto,
    pre.Revenda,
    rev.Bandeira,
    pre.Valor_venda,
    pre.Unidade_medida,
    pre.Data_coleta
FROM estados AS es
LEFT JOIN municipio AS mun
	ON es.Estado_id = mun.Estado_id
LEFT JOIN revendas AS rev 
	ON mun.Municipio_id = rev.Municipio_id
LEFT JOIN precos AS pre
	ON rev.Revenda_id = pre.Revenda_id
LEFT JOIN endereco AS ende
		ON mun.Municipio_id = ende.Municipio_id
WHERE es.Estado_sigla = "BA" AND mun.Municipio = "Salvador"