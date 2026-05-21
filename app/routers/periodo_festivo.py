import logging as log 
from sqlalchemy import text
from fastapi import HTTPException, Depends, APIRouter
from app.dependencies import get_db

logger = log.getLogger(__name__)
router = APIRouter()

@router.get("/periodo-festivo/revellion")

def preco_revellion(db= Depends(get_db)):
    
    try:
        query = text(
            '''
                SELECT 
                    loc.municipio,
                    loc.bairro,
                    ROUND(AVG(fat.valor_venda), 2) AS preco_media,
                    tmp.data_coleta,
                    pro.produto
                FROM fato_preco AS fat
                JOIN dim_localizacao AS loc
                    ON fat.id_localizacao = loc.id_localizacao 
                JOIN dim_tempo AS tmp
                    ON fat.id_tempo = tmp.id_tempo 
                JOIN dim_produto AS pro
                    ON fat.id_produto = pro.id_produto 
                WHERE tmp.data_coleta BETWEEN '2025-01-01' AND '2025-01-03' AND loc.municipio = 'Salvador'
                GROUP BY loc.bairro, tmp.data_coleta, pro.produto, loc.municipio 
           
            '''
             
        )
        
        result = db.execute(query)
        logger.info("Consulta realizada a média de preços durante o periodo de reveillon")
        return result.mappings().all()
    
    except Exception:
        logger.exception("Erro ao consultar média de preços durante o periodo de reveillon")
        raise HTTPException(status_code= 500, detail= "Erro ao consultar média de preços durante o periodo de reveillon" )
    
    
@router.get("/periodo-festivo/carnaval")

def preco_carnaval(db = Depends(get_db)):
    
    try:
        
        query = text(
            '''
                SELECT 
                loc.municipio,
                loc.bairro,
                ROUND(AVG(fat.valor_venda), 2) AS preco_media,
                tmp.data_coleta,
                pro.produto
            FROM fato_preco AS fat
            JOIN dim_localizacao AS loc
                ON fat.id_localizacao = loc.id_localizacao 
            JOIN dim_tempo AS tmp
                ON fat.id_tempo = tmp.id_tempo 
            JOIN dim_produto AS pro
                ON fat.id_produto = pro.id_produto 
            WHERE tmp.data_coleta BETWEEN '2025-02-27' AND '2025-03-04' AND loc.municipio = 'Salvador'
            GROUP BY loc.bairro, tmp.data_coleta, pro.produto, loc.municipio 
            
            '''
            
        )
        
        result = db.execute(query)
        logger.info("Consulta realizada a média de preços durante o periodo de carnaval")
        return result.mappings().all()
    
    except Exception:
        logger.exception("Erro ao consultar média de preços durante o periodo de carnaval")
        raise HTTPException(status_code= 500, detail= "Erro ao consultar média de preços durante o periodo de carnaval" )
    

@router.get("/periodo-festivo/festas-juninas")

def preco_festas_juninas(db = Depends(get_db)):
    
    try:
        
        query = text(
            '''
            SELECT 
                loc.municipio,
                loc.bairro,
                ROUND(AVG(fat.valor_venda), 2) AS preco_media,
                tmp.data_coleta,
                pro.produto
            FROM fato_preco AS fat
            JOIN dim_localizacao AS loc
                ON fat.id_localizacao = loc.id_localizacao 
            JOIN dim_tempo AS tmp
                ON fat.id_tempo = tmp.id_tempo 
            JOIN dim_produto AS pro
                ON fat.id_produto = pro.id_produto 
            WHERE tmp.data_coleta BETWEEN '2025-06-20' AND '2025-06-26' AND loc.municipio = 'Salvador'
            GROUP BY loc.bairro, tmp.data_coleta, pro.produto, loc.municipio 
            
            '''
            
        )
        
        result = db.execute(query)
        logger.info("Consulta realizada a média de preços durante o periodo de festas juninas")    
        return result.mappings().all()
    
    except Exception:
        logger.exception("Erro ao consultar média de preços durante o periodo de festas juninas")
        raise HTTPException(status_code= 500, detail= "Erro ao consultar média de preços durante o periodo de festas juninas" )
        
@router.get("/periodo-festivo/natal")

def preco_natal(db = Depends(get_db)):
    
    try:
        query = text(
            '''
            SELECT 
                loc.municipio,
                loc.bairro,
                ROUND(AVG(fat.valor_venda), 2) AS preco_media,
                tmp.data_coleta,
                pro.produto
            FROM fato_preco AS fat
            JOIN dim_localizacao AS loc
                ON fat.id_localizacao = loc.id_localizacao 
            JOIN dim_tempo AS tmp
                ON fat.id_tempo = tmp.id_tempo 
            JOIN dim_produto AS pro
                ON fat.id_produto = pro.id_produto 
            WHERE tmp.data_coleta BETWEEN '2025-12-22' AND '2025-12-28' AND loc.municipio = 'Salvador'
            GROUP BY loc.bairro, tmp.data_coleta, pro.produto, loc.municipio
            
            '''
            
            
        )
        
        result = db.execute(query)
        logger.info("Consulta realizada a média de preços durante o periodo de festas natalinas")
        return result.mappings().all()
    
    except Exception:
        logger.exception("Erro ao consultar média de preços durante o periodo de festas natalinas")
        raise HTTPException(status_code= 500, detail= "Erro ao consultar média de preços durante o periodo de festas natalinas" )
