import pandas as pd 
import os
from dotenv import load_dotenv

load_dotenv()
caminho_semestre1 = os.getenv("CSV_PATH_01")
caminho_semestre2 = os.getenv("CSV_PATH_02")

df_semestre1 = pd.read_csv(caminho_semestre1, delimiter=';', encoding="latin1")
df_semestre2 = pd.read_csv(caminho_semestre2, delimiter=';', encoding="latin1")

df = pd.concat([df_semestre1, df_semestre2], ignore_index=True)
caminho_pasta = os.getenv("DIR_PATH_CSV")
nome_arquivo = "Preços anuais - AUTOMOTIVOS_2025.csv"
caminho_completo = os.path.join(caminho_pasta, nome_arquivo)
df.to_csv(caminho_completo, sep=";", encoding="latin1", index= False)
print("Arquivo gerado")