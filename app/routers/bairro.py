import logging as log 
from app.dependencies import get_db
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy import text 

logger = log.getLogger(__name__)
router = APIRouter()

@router.get("/bairro/media")

def media_bairro(db = Depends(get_db)):

    try:
        query = text(
            
        '''
            SELECT 
                loc.estado_sigla,
                loc.municipio,
                loc.bairro,
                ROUND(AVG(fat.valor_venda), 2) AS media_preco
            FROM fato_preco AS fat
            JOIN dim_localizacao AS loc
                ON fat.id_localizacao = loc.id_localizacao 
            WHERE loc.municipio = 'Salvador'
                AND loc.bairro IN ('Pituba', 'Caminho Das Arvores', 'Brotas', 'Engenho Velho De Brotas',
                'Paralela', 'Cajazeiras', 'Cabula', 'Itapua')
            GROUP BY 
                loc.bairro, 
                loc.municipio, 
                loc.estado_sigla
    
        '''
        )
        
        result = db.execute(query)
        logger.info("Media de preço por bairro")
        return result.mappings().all()
    
    except Exception:
        logger.exception(f"Erro ao consultar media de preço por bairro")
        raise HTTPException(status_code= 500, detail="Erro ao consultar media de preço por bairro")

@router.get("/bairro/mais-caro")

def bairro_preco_caro(db = Depends(get_db)):
    try:
        query = text(
            '''
                SELECT 
                    loc.estado_sigla,
                    loc.municipio,
                    loc.bairro,
                    ROUND(AVG(fat.valor_venda), 2) AS preco_medio,
                    pro.produto,
                    pro.unidade_medida
                FROM fato_preco AS fat
                JOIN dim_localizacao AS loc
                    ON fat.id_localizacao = loc.id_localizacao 
                JOIN dim_produto AS pro
                    ON fat.id_produto = pro.id_produto 
                WHERE loc.municipio = 'Salvador'
                GROUP BY 
                    loc.bairro, 
                    loc.municipio, 
                    loc.estado_sigla, 
                    pro.produto, 
                    pro.unidade_medida
                ORDER BY preco_medio DESC 

            '''
            
        )

        result = db.execute(query)
        logger.info("Consulta realizada a preços mais caros por bairro")
        return result.mappings().all()
    
    except Exception:
        logger.exception("Erro ao consultar preços mais caros por bairro")
        raise HTTPException(status_code= 500, detail= "Erro ao consultar preços mais caros por bairro")



@router.get("/bairro/mais-barato")

def bairro_preco_barato(db= Depends(get_db)):
    
    try:
        query = text(
            '''
            SELECT 
                loc.estado_sigla,
                loc.municipio,
                loc.bairro,
                ROUND(AVG(fat.valor_venda), 2) AS preco_medio,
                pro.produto,
                pro.unidade_medida
            FROM fato_preco AS fat
            JOIN dim_localizacao AS loc
                ON fat.id_localizacao = loc.id_localizacao 
            JOIN dim_produto AS pro
                ON fat.id_produto = pro.id_produto 
            WHERE loc.municipio = 'Salvador'
            GROUP BY 
                loc.bairro, 
                loc.municipio, 
                loc.estado_sigla, 
                pro.produto, 
                pro.unidade_medida
            ORDER BY preco_medio ASC
            
            '''
                
        )
        
        result = db.execute(query)
        logger.info("Consulta realizada a preços mais baratos por bairro")
        return result.mappings().all()
    
    except Exception:
        logger.exception("Erro ao consultar preços mais baratos por bairro")
        raise HTTPException(status_code= 500, detail= "Erro ao consultar preços mais baratos por bairro")