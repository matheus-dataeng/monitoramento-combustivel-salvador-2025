import logging as log 
from app.dependencies import get_db
from fastapi import HTTPException, Depends, APIRouter
from sqlalchemy import text 

logger = log.getLogger(__name__)
router = APIRouter()

@router.get("/postos")

def postos(db = Depends(get_db)):
    
    try:
        
        query = text(
            '''
            SELECT 
                rev.revenda,
                loc.estado_sigla,
                loc.municipio,
                loc.bairro
            FROM fato_preco AS fat
            JOIN dim_revenda AS rev
                ON fat.id_revenda = rev.id_revenda
            JOIN dim_localizacao AS loc 
                ON fat.id_localizacao = loc.id_localizacao
            WHERE loc.municipio = 'Salvador'
	  
            '''
            
        )
        
        result = db.execute(query)
        logger.info("Consulta realizada a postos de abastecimento")
        return result.mappings().all()
    
    except Exception:
        logger.exception("Erro ao consultar postos de abastecimento")
        raise HTTPException(status_code= 500, detail="Erro ao consultar postos de abastecimento")
    
@router.get("/postos/mais-caros")

def postos_mais_caros(db = Depends(get_db)):
    
    try:
        
        query = text(
            '''
            SELECT DISTINCT
                rev.revenda,
                loc.estado_sigla,
                loc.municipio,
                loc.bairro,
                ROUND(AVG(fat.valor_venda), 2) AS valor_total
            FROM fato_preco AS fat
            JOIN dim_revenda AS rev
                ON fat.id_revenda = rev.id_revenda
            JOIN dim_localizacao AS loc 
                ON fat.id_localizacao = loc.id_localizacao
            WHERE loc.municipio = 'Salvador'
            GROUP BY rev.revenda, loc.estado_sigla, loc.municipio, loc.bairro
            ORDER BY valor_total DESC
            
            
            '''
            
        )
        
        result = db.execute(query)
        logger.info("Consulta realizada a postos de abastecimento com valor mais alto")
        return result.mappings().all()
    
    except Exception:
        logger.exception("Erro ao consultar postos de abastecimentos com valor mais alto")
        raise HTTPException(status_code= 500, detail="Erro ao consultar postos de abastecimentos com valor mais alto")
    
@router.get("/postos/mais-barato")

def postos_mais_baratos(db = Depends(get_db)):
    
    try:
        
        query = text(
            '''
            SELECT 
                rev.revenda,
                loc.estado_sigla,
                loc.municipio,
                loc.bairro,
                ROUND(AVG(fat.valor_venda), 2) AS valor_total
            FROM fato_preco AS fat
            JOIN dim_revenda AS rev
                ON fat.id_revenda = rev.id_revenda
            JOIN dim_localizacao AS loc 
                ON fat.id_localizacao = loc.id_localizacao
            WHERE loc.municipio = 'Salvador'
            GROUP BY rev.revenda, loc.estado_sigla, loc.municipio, loc.bairro
            ORDER BY valor_total ASC
            
            '''
            
        )
        
        result = db.execute(query)
        logger.info("Consulta realizada a postos de abastecimento com valor mais baixo")
        return result.mappings().all()
    
    except Exception:
        logger.exception("Erro ao consultar postos de abastecimentos com valor mais baixo")
        raise HTTPException(status_code= 500, detail="Erro ao consultar postos de abastecimentos com valor mais baixo")



        