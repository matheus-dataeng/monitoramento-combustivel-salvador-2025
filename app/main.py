from fastapi import FastAPI
from src.utils.logger_config import log_setup
from app.routers.bairro import router as bairro
from app.routers.periodo_festivo import router as periodo_festivo
from app.routers.postos import router as postos
from app.routers.preco import router as preco

log_setup()

app = FastAPI(
    title= "Monitoramento dos preços dos combustíveis em Salvador durante 2025",
    version= "1.0.0"
)

app.include_router(
    bairro,
    tags=["Bairros 🏘️"]
)

app.include_router(
    periodo_festivo,
    tags=["Periodo Festivo 🎉"]
)

app.include_router(
    postos,
    tags=["Postos ⛽"]
)

app.include_router(
    preco,
    tags=["Preços 💰"]
)