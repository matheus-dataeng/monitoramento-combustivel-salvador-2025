SELECT 
	es.Estado_id,
	es.Estado_sigla,
    mun.Municipio,
    pre.Produto,
    pre.Valor_venda,
    rev.Revenda,
    rev.CNPJ,
    rev.Bandeira,
    pre.Data_coleta
FROM estados AS es
LEFT JOIN municipio AS mun
		ON es.Estado_sigla = mun.Estado_sigla
LEFT JOIN revendas AS rev
    ON mun.Municipio_id = rev.Municipio_id        
LEFT JOIN precos AS pre
	ON rev.Revenda_id = pre.Revenda_id
WHERE es.Estado_sigla = "BA" AND mun.Municipio = "Salvador"
