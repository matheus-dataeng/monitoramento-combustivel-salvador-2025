from __future__ import annotations


import pandas as pd 
import logging as log 
from pathlib import Path
from typing import Dict
from src.s3_load import s3_upload

logger = log.getLogger(__name__)

def dim_tempo(df: pd.DataFrame) -> pd.DataFrame:
    
    df_tempo  = df[["data_coleta"]].drop_duplicates().reset_index(drop=True)
    
    df_tempo["id_tempo"] = df_tempo.index +1
    df_tempo["ano"] = df_tempo["data_coleta"].dt.year
    df_tempo["mes"] = df_tempo["data_coleta"].dt.month
    df_tempo["dia"] = df_tempo["data_coleta"].dt.day
    
    dia_semana = {
        "Monday" : "Segunda-Feira",
        "Tuesday" : "Terça-Feira",
        "Wednesday" : "Quarta-Feira",
        "Thursday" : "Quinta-Feira",
        "Friday" : "Sexta-Feira",
        "Saturday" : "Sábado",
        "Sunday" : "Domingo"
    }
    
    df_tempo["dia_semana"] = df_tempo["data_coleta"].dt.day_name().replace(dia_semana)
    
    df_tempo = df_tempo[[
        "id_tempo",
        "data_coleta",
        "ano",
        "mes",
        "dia",
        "dia_semana"
    ]]
    
    logger.info(f"Dimensão tempo criada / Colunas: {df_tempo.shape[1]} / Linhas: {len(df_tempo)}")
    
    return df_tempo

def dim_revenda(df: pd.DataFrame) -> pd.DataFrame:
    
    df_revenda = df[[
        "revenda",
        "cnpj_revenda",
        "bandeira"
    ]].drop_duplicates().reset_index(drop=True)
    
    df_revenda["id_revenda"] = df_revenda.index +1
    
    df_revenda = df_revenda[[
        "id_revenda",
        "revenda",
        "cnpj_revenda",
        "bandeira"
    ]]
    
    logger.info(f"Dimensão revenda criada / Colunas: {df_revenda.shape[1]} / Linhas: {len(df_revenda)}")
    
    return df_revenda

def dim_produto(df: pd.DataFrame) -> pd.DataFrame:

    df_produto = df[[
        "produto",
        "unidade_medida",
    ]].drop_duplicates().reset_index(drop=True)
    
    df_produto["id_produto"] = df_produto.index +1
    
    df_produto = df_produto[[
        "id_produto",
        "produto",
        "unidade_medida"
    ]]
    
    logger.info(f"Dimensão produto criada / Colunas: {df_produto.shape[1]} / Linhas: {len(df_produto)}")
    
    return df_produto

def dim_localizacao(df: pd.DataFrame) -> pd.DataFrame:
    
    df_localizacao = df[[
        "regiao_sigla",
        "estado_sigla",
        "municipio",
        "bairro"
    ]].drop_duplicates().reset_index(drop=True)

    df_localizacao["id_localizacao"] = df_localizacao.index +1
    
    df_localizacao = df_localizacao[[
        "id_localizacao",
        "regiao_sigla",
        "estado_sigla",
        "municipio",
        "bairro"
    ]]
    
    logger.info(f"Dimensão localização criada / Colunas: {df_localizacao.shape[1]} / Linhas: {len(df_localizacao)}")
    
    return df_localizacao
    
def fato(
    df: pd.DataFrame,
    df_tempo: pd.DataFrame,
    df_revenda: pd.DataFrame,
    df_produto: pd.DataFrame,
    df_localizacao: pd.DataFrame
) -> pd.DataFrame:
    
    fato_preco = df.merge(
        df_tempo,
        on= ["data_coleta"],
        how= "left"
    )
    
    fato_preco = fato_preco.merge(
        df_revenda,
        on= ["revenda", "cnpj_revenda", "bandeira"],
        how= "left"
    )
    
    fato_preco = fato_preco.merge(
        df_produto,
        on= ["produto", "unidade_medida"],
        how= "left"
    )
    
    fato_preco = fato_preco.merge(
        df_localizacao,
        on= ["regiao_sigla", "estado_sigla", "municipio", "bairro"],
        how= "left"
    )

    fato_preco = fato_preco[[
        "id_tempo", 
        "id_revenda", 
        "id_produto", 
        "id_localizacao",
        "valor_venda"
    ]].drop_duplicates().reset_index(drop=True)
    
    logger.info(f"Fato criada com sucesso / Colunas: {fato_preco.shape[1]} / Linhas: {len(fato_preco)}")
    
    return fato_preco

def build_metrics(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    
    logger.info("Iniciando criação das metricas") 
    
    df_tempo = dim_tempo(df)
    df_revenda = dim_revenda(df)
    df_produto = dim_produto(df)
    df_localizacao = dim_localizacao(df)
    fato_preco = fato(
        df,
        df_tempo,
        df_revenda,
        df_produto,
        df_localizacao      
    )
    
    return {
        "dim_tempo" : df_tempo,
        "dim_revenda" : df_revenda,
        "dim_produto" : df_produto,
        "dim_localizacao" : df_localizacao,
        "fato_preco" : fato_preco
    }

def path_datalake_gold(nome_df: str, df: pd.DataFrame) -> None:
    
    gold_path = Path(f"data_lake/gold/{nome_df}.parquet")
    gold_path.parent.mkdir(parents= True, exist_ok=True)
    
    try:
        df.to_parquet(gold_path, index= False)
        s3_upload(gold_path, f"gold/{nome_df}.parquet")
        
    except Exception as e:
        logger.error(f"Erro ao salvar {nome_df}: {e}")
        raise 
    
def load_gold_datalake(tabelas: Dict[str, pd.DataFrame]) -> None:
    
    logger.info("Iniciando carga datalake gold")
    
    try:
        for nome_df, df in tabelas.items():
            path_datalake_gold(nome_df, df)
            logger.info("Arquivos carregados")
                
    except Exception:
        logger.exception("Erro ao subir arquivos")
        raise 