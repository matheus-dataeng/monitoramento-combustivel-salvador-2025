
CREATE TABLE dim_tempo (
    id_tempo        INT PRIMARY KEY,
    data_coleta     DATE NOT NULL,
    ano             INTEGER NOT NULL,
    mes             INTEGER NOT NULL,
    dia             INTEGER NOT NULL,
    dia_semana      VARCHAR(20) NOT NULL
);

CREATE TABLE dim_revenda (
    id_revenda      INT PRIMARY KEY,
    revenda         VARCHAR(255),
    cnpj_revenda    VARCHAR(20),
    bandeira        VARCHAR(100)
);

CREATE TABLE dim_produto (
    id_produto      INT PRIMARY KEY,
    produto         VARCHAR(100) NOT NULL,
    unidade_medida  VARCHAR(20)
);

CREATE TABLE dim_localizacao (
    id_localizacao  INT PRIMARY KEY,
    regiao_sigla    VARCHAR(5),
    estado_sigla    VARCHAR(5),
    municipio       VARCHAR(100),
    bairro          VARCHAR(100)
);


CREATE TABLE fato_preco (
    id_tempo        INT NOT NULL REFERENCES dim_tempo(id_tempo),
    id_revenda      INT NOT NULL REFERENCES dim_revenda(id_revenda),
    id_produto      INT NOT NULL REFERENCES dim_produto(id_produto),
    id_localizacao  INT NOT NULL REFERENCES dim_localizacao(id_localizacao),
    valor_venda     NUMERIC(10, 3)
)


