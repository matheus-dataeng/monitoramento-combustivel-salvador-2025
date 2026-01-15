import pandas as pd
import os 
import mysql.connector
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

load_dotenv()
conexao_banco = mysql.connector.connect(
    user = os.getenv("USER_DB"),
    password = os.getenv("PASSWORD_DB"),
    host = os.getenv("HOST_DB"),
    port = os.getenv("PORT_DB"),
    database = "Combustivel_2025_geral"
)

cursor = conexao_banco.cursor()
query = '''

   SELECT  
	es.Estado_id,
    es.Estado_sigla,
    mun.Municipio,
    ende.Bairro,
    pre.Produto,
    pre.Valor_venda,
    pre.Status_preco,
    pre.Unidade_medida,
    rev.Revenda,
    ende.CEP,
    pre.Data_coleta,
    CASE
		WHEN MONTH(pre.Data_coleta) = 6
			THEN 'Festas Juninas'
        WHEN MONTH(pre.Data_coleta) IN (1, 12)
			THEN 'Festa de Fim de Ano'
    END AS Periodo_festivo        
FROM estado AS es
LEFT JOIN municipio AS mun
	ON es.Estado_id = mun.Estado_id
LEFT JOIN revendas AS rev
	ON mun.Municipio_id = rev.Municipio_id
LEFT JOIN precos AS pre 
	ON rev.Revenda_id = pre.Revenda_id
LEFT JOIN endereco AS ende
		ON mun.Municipio_id = ende.Municipio_id
WHERE es.Estado_sigla = "BA" AND mun.Municipio = "Salvador" AND MONTH(pre.Data_coleta) IN (1, 6, 12)
ORDER BY RAND()
LIMIT 100

'''

df = pd.read_sql(query, conexao_banco)
conexao_banco.close()

credencial_json = r"C:\Users\mathe\Downloads\pythonpraticas-1b22846ff6a0.json"
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name(credencial_json, scope)
google_sheets = gspread.authorize(credentials)
planilha = google_sheets.open_by_key("1y2YbRejTAGPNrk7K1_wBGYQs-Ef0oDrodx-EC5baOm4")
planilha_guia = planilha.worksheet("Monitoramento")

set_with_dataframe(
    planilha_guia,
    df,
    include_index= False,
    include_column_header= True,
    resize= True

)

print("Carga Realizada")
