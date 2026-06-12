import requests 

URL_API = "http://api:8000"

def get_media_geral():
    
    response = requests.get(
        f"{URL_API}/precos/media-geral"
    )
    
    return response.json()

def get_bairro():
    
    response = requests.get(
        f"{URL_API}/bairro/media"
    )
    
    return response.json()

def get_revellion():
    
    response = requests.get(
        f"{URL_API}/periodo-festivo/revellion"
    )
    
    return response.json()

def get_carnaval():
    
    response = requests.get(
        f"{URL_API}/periodo-festivo/carnaval"
    )
    
    return response.json()

def get_festas_juninas():
    
    response = requests.get(
        f"{URL_API}/periodo-festivo/festas-juninas"
    )
    
    return response.json()

def get_natal():
    
    response = requests.get(
        f"{URL_API}/periodo-festivo/natal"
    )
    
    return response.json()