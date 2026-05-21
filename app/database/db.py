import logging as log 
from dotenv import load_dotenv
import os 
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logger = log.getLogger(__name__)
load_dotenv()

user = os.getenv("USER_PG")
password = os.getenv("PASSWORD_PG")
host = os.getenv("HOST_PG")
port  = os.getenv("PORT_PG")
dbname = os.getenv("DBNAME_PG")

if not all([user, password, host, port, dbname]):
    logger.error("Credencias não definidas no arquivo .env")
    raise ValueError("Variaveis não definidas no .env") 

try:
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(url)
    SessionLocal= sessionmaker(autocommit= False, autoflush=False, bind= engine)
    logger.info("Conexão Ok")

except Exception:
    logger.exception("Erro ao criar conexão")