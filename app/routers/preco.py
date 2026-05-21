import logging as log 
from sqlalchemy import text 
from app.dependencies import get_db
from fastapi import APIRouter, HTTPException, Depends

logger = log.getLogger(__name__)
router = APIRouter()

@router.get("/precos/media-geral")

def media_geral(db = Depends(get_db)):
    
    try: 
        
        query = text(
            '''
                SELECT
                    pro.produto,
                    pro.unidade_medida,
                    ROUND(AVG(fat.valor_venda), 2) AS media_preco
                FROM fato_preco AS fat
                JOIN dim_produto AS pro
                    ON fat.id_produto = pro.id_produto
                JOIN dim_tempo AS tmp
                    ON fat.id_tempo = tmp.id_tempo
                JOIN dim_localizacao AS loc 
                    ON fat.id_localizacao = loc.id_localizacao
                WHERE loc.municipio = 'Salvador'
                GROUP BY pro.produto, pro.unidade_medida
            
            
            '''
            
        )
        
        result = db.execute(query)
        logger.info("Consulta realizada a media de preços")
        return result.mappings().all()
        
    except Exception:
        logger.exception("Erro ao consultar media de preços")
        raise HTTPException(status_code= 500, detail="Erro ao consultar media de preços")

