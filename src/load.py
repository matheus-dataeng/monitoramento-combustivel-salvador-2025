import pandas as pd 
import logging as log
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv 

logger = log.getLogger(__name__)
load_dotenv()

def load(
    df_tempo: pd.DataFrame,
    df_revenda: pd.DataFrame,
    df_produto: pd.DataFrame,
    df_localizacao: pd.DataFrame,
    fato_preco: pd.DataFrame
    
) -> None:
    
    user = os.getenv("USER_PG")
    password = os.getenv("PASSWORD_PG")
    host = os.getenv("HOST_PG")
    port = os.getenv("PORT_PG")
    dbname = os.getenv("DBNAME_PG")
    
    if not all([user, password, host, port, dbname]):
        logger.error("Credencias do banco não definidas no .env")
        raise ValueError("Variaveis de ambiente não definidas")
    
    try:
        url_banco = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(url_banco)
        
        table_dim_tempo = os.getenv("TABLE_DIM_TEMPO")
        table_dim_revenda = os.getenv("TABLE_DIM_REVENDA")
        table_dim_produto = os.getenv("TABLE_DIM_PRODUTO")
        table_dim_localizacao = os.getenv("TABLE_DIM_LOCALIZACAO")
        table_fato_preco = os.getenv("TABLE_FATO_PRECO")
        
        if not all([
            table_dim_tempo,
            table_dim_revenda,
            table_dim_produto,
            table_dim_localizacao,
            table_fato_preco
        ]):
            
            logger.error("Nomes das tabelas não definidas no .env")
            raise ValueError("Variaveis de ambiente não definidas")
        
        tabelas = [
            
            {
                "df" : df_tempo,
                "tabela" : table_dim_tempo,
                
            },
            
            {
                "df" : df_revenda,
                "tabela" : table_dim_revenda,
            },
            
            {
                "df" : df_produto,
                "tabela" : table_dim_produto,
            },
            
            {
                "df" : df_localizacao,
                "tabela" : table_dim_localizacao,
            },
               
        ]
        
        with engine.begin() as conn:
            
            conn.execute(text(f'TRUNCATE TABLE {table_fato_preco} CASCADE'))
            logger.info(f"Tabela truncada: {table_fato_preco}")
            
            for elem in tabelas:
                df = elem["df"]
                tabela = elem["tabela"]
                
                conn.execute(text(f'TRUNCATE TABLE {tabela} CASCADE'))
                logger.info(f"Tabela truncada: {tabela}")
                
                df.to_sql(name = tabela, con = conn, index = False, chunksize = 10000, if_exists = "append", )
                logger.info(f"Tabela carregada: {tabela} / Colunas: {df.shape[1]} / Linhas: {len(df)}")
                
            fato_preco.to_sql(name = table_fato_preco, con= conn, index= False, chunksize= 10000, if_exists= "append", method = "multi")
            logger.info(f"Tabela carregada: {table_fato_preco} / Colunas: {fato_preco.shape[1]} / Linhas: {len(fato_preco)}")
    
    except Exception:
        logger.exception("Falha no Load")
        raise 