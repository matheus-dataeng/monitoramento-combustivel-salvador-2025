import pandas as pd 
import logging as log 
import os 
from dotenv import load_dotenv
from pathlib import Path
from src.s3_load import s3_upload

logger = log.getLogger(__name__)
load_dotenv()

def concat_files(df_semestre_1: pd.DataFrame, df_semestre_2: pd.DataFrame) -> pd.DataFrame:
    
    logger.info("Concatenando arquivos")
    
    try:
        df = pd.concat([df_semestre_1, df_semestre_2], ignore_index= True)
        caminho_pasta = os.getenv("DIR_PATH_CSV")
        
        if not caminho_pasta:
            logger.error("Caminho não definido no arquivo .env")
            raise
        
        nome_arquivo = "Preços anuais - AUTOMOTIVOS_2025.csv"
        caminho_completo = os.path.join(caminho_pasta, nome_arquivo)
        df.to_csv(caminho_completo, sep=";", encoding="latin1", index= False)
        logger.info("Arquivo gerado")
    
    except Exception:
        logger.exception("Erro ao gerar arquivo")
        raise 
    
    return df

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    
    logger.info("Renomeando arquivos")
    
    df = df[[
        "ï»¿Regiao - Sigla", 
        "Estado - Sigla",
        "Municipio",
        "Revenda",
        "CNPJ da Revenda",
        "Nome da Rua",
        "Numero Rua",
        "Complemento",
        "Bairro",
        "Cep",
        "Produto",
        "Data da Coleta",
        "Valor de Venda",
        "Unidade de Medida",
        "Bandeira"
    ]]
    
    try:    
        columns_rename = {
            "ï»¿Regiao - Sigla" : "regiao_sigla" ,
            "Estado - Sigla" : "estado_sigla",
            "Municipio" : "municipio",
            "Revenda" : "revenda",
            "CNPJ da Revenda" : "cnpj_revenda",
            "Nome da Rua" : "nome_rua",
            "Numero Rua" : "numero_rua",
            "Complemento" : "complemento",
            "Bairro" : "bairro",
            "Cep" : "cep",
            "Produto" : "produto",
            "Data da Coleta" : "data_coleta",
            "Valor de Venda" : "valor_venda",
            "Unidade de Medida" : "unidade_medida",
            "Bandeira" : "bandeira"
        }
        
        df.rename(columns=columns_rename, inplace= True)
        logger.info(f"Colunas renomeadas / Colunas: {df.shape[1]} / Linhas: {len(df)}")
        
    except Exception:
        logger.exception("Erro ao renomear colunas")
        raise 
    
    return df

def remove_diplicates(df: pd.DataFrame) -> pd.DataFrame:
    
    logger.info("Removendo dados duplicados")
    
    try:
        linhas_antes = len(df)
        df = df.drop_duplicates()
        linhas_depois = len(df)
        
        logger.warning(f"Dados removidos: {linhas_antes - linhas_depois} / Linhas restantes: {linhas_depois}")
    
    except Exception:
        logger.exception("Erro ao remover dados duplicados")
        raise 
    
    return df

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    
    logger.info("Iniciando validações")
    
    cols_str = [
        "municipio", 
        "revenda", 
        "cnpj_revenda", 
        "nome_rua", 
        "complemento", 
        "bairro", 
        "cep", 
        "produto", 
        "bandeira"
    ]
    
    try:
        for cols in cols_str:
            if cols in df.columns:
                df[cols] = df[cols].fillna("Não informado").str.strip().str.title()
        
        df["valor_venda"] = df["valor_venda"].astype(str).str.replace(",", ".", regex= True)
        df["valor_venda"] = pd.to_numeric(df["valor_venda"], errors= "coerce")
                
        valor_venda_invalido = int(df["valor_venda"].isna().sum())
        
        if valor_venda_invalido:
            logger.warning(f"Valores invalidos: {valor_venda_invalido}")
            df = df.dropna(subset=["valor_venda"])
                
        logger.info(f"Validações realizadas / Colunas: {df.shape[1]} / Linhas: {len(df)}")
    
    except Exception:  
        logger.exception("Erro ao validar dados") 
        raise 
    
    return df         

def validate_dates(df: pd.DataFrame) -> pd.DataFrame:
    
    logger.info("Iniciando validações de datas")

    try:
        df["data_coleta"] = pd.to_datetime(df["data_coleta"], dayfirst= True, errors= "coerce")
        
        datas_invalidas = int(df["data_coleta"].isna().sum())
        
        if datas_invalidas:
            logger.warning(f"Datas invalidas: {datas_invalidas}")
            df = df.dropna(subset=["data_coleta"])
        
        logger.info(f"Datas validadas / Linhas: {len(df)}")
    
    except Exception:
        logger.exception("Erro ao validar datas")
        raise 
    
    return df

def transform(df_semestre_1: pd.DataFrame, df_semestre_2: pd.DataFrame) -> pd.DataFrame:
    
    logger.info("Iniciando transformações")
    
    df = concat_files(df_semestre_1, df_semestre_2)
    df = rename_columns(df)
    df = remove_diplicates(df)
    df = validate_data(df)
    df = validate_dates(df)
    
    logger.info(f"Transformações realizadas / Colunas: {df.shape[1]} / Linhas: {len(df)}")
   
    return df

def load_silver_datalake(df: pd.DataFrame) -> None:
    
    logger.info("Iniciando carga no datalake silver")
    
    try:
        silver_path = Path("data_lake/silver/analise_geral_2025.parquet")
        silver_path.parent.mkdir(parents= True, exist_ok= True)
        df.to_parquet(silver_path, index= False)
        s3_upload(silver_path, "silver/analise_geral_2025.parquet")
    
    except Exception:
        logger.exception("Erro ao realizar carga")
        raise 
