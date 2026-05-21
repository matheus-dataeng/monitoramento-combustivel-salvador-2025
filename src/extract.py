from __future__ import annotations

import pandas as pd 
import logging as log 
from pathlib import Path
from dotenv import load_dotenv
import os
from src.s3_load import s3_upload
from typing import Dict

logger = log.getLogger(__name__)
load_dotenv()

def extract() -> Dict[str, pd.DataFrame]:
    
    logger.info("Iniciando extração de arquivos")
    
    try:
        semestre_1 = os.getenv("CSV_PATH_01")
        semestre_2 = os.getenv("CSV_PATH_02")
    
        if not semestre_1 or not semestre_2:
            logger.error("Arquivos não definidos no .env")
            raise ValueError("Variaveis de ambiente não definidas")
        
        df_semestre_1 = pd.read_csv(semestre_1, encoding= "latin1", low_memory= False, delimiter= ";")
        df_semestre_2 = pd.read_csv(semestre_2, encoding= "latin1", low_memory= False, delimiter= ";")
        
        logger.info(
            f"Arquivos extraidos",
            extra={
                "Colunas semestre_1" : df_semestre_1.shape[1],
                "Linhas semestre_1" :  len(df_semestre_1),
                "Colunas semestre_2" : df_semestre_2.shape[1],
                "Linhas semestre_2" :  len(df_semestre_2),
            }
        ) 
        
    except Exception as e:
        logger.exception(f"Erro ao extrair arquivos: {e}")
        raise 
    
    return {
        "analise_semestre_1" : df_semestre_1,
        "analise_semestre_2" : df_semestre_2
    }
    
def path_bronze_datalake(nome_df: str, df: pd.DataFrame) -> None:
    
    bronze_path = Path(f"data_lake/bronze/{nome_df}.parquet")
    bronze_path.parent.mkdir(parents= True, exist_ok= True)
    
    try:
        df.to_parquet(bronze_path, index= False)
        s3_upload(bronze_path, f"bronze/{nome_df}.parquet")
    
    except Exception as e:
        logger.error(f"Erro ao salvar {nome_df}: {e}")
        raise  

def load_bronze_datalake(tabelas: Dict[str, pd.DataFrame]) -> None:

    logger.info("Iniciando carga datalake bronze")
    
    try:
        for nome_df, df in tabelas.items():
            path_bronze_datalake(nome_df, df)

        logger.info("Arquivos carregados")
        
    except Exception:
        logger.exception(f"Erro ao subir arquivos")
        raise 