import pandas as pd 
from sqlalchemy import create_engine, Integer, VARCHAR, Float, Date
import os
from dotenv import load_dotenv

load_dotenv()
caminho = os.getenv("CSV_PATH_02")
df = pd.read_csv(caminho, delimiter=';', encoding= "latin1")

#DATAFRAME ESTADOS
df_estados = df[["ï»¿Regiao - Sigla", "Estado - Sigla"]].drop_duplicates()

colunas_estados_tratadas = {
    "ï»¿Regiao - Sigla" : "Regiao_sigla",
    "Estado - Sigla" : "Estado_sigla"
}

df_estados.rename(columns=colunas_estados_tratadas, inplace=True)
df_estados['Estado_id'] = df_estados.index +1
df_estados = df_estados[['Estado_id', "Regiao_sigla", "Estado_sigla"]]

#DATAFRAME MUNICIPIO
df_municipio = df[["Municipio", "Estado - Sigla"]].drop_duplicates()
df_municipio['Municipio'] = df_municipio['Municipio'].str.title()
df_municipio['Municipio'] = df_municipio['Municipio'].str.strip() 
colunas_municipios_tratadas = {"Estado - Sigla" : "Estado_sigla"}
df_municipio.rename(columns=colunas_municipios_tratadas, inplace=True,)
df_municipio['Municipio_id'] = df_municipio.index +1
df_municipio = df_municipio[["Municipio_id", "Municipio" ,"Estado_sigla"]]

#DATAFRAME REVENDAS
df_revendas = df[["Revenda", "CNPJ da Revenda", "Bandeira", "Municipio"]].drop_duplicates()
colunas_revendas_tratadas = {"CNPJ da Revenda" : "CNPJ"}
df_revendas.rename(columns=colunas_revendas_tratadas, inplace= True)
df_revendas['Revenda'] = df_revendas['Revenda'].str.title()
df_revendas['CNPJ'] = df_revendas['CNPJ'].astype(str)
df_revendas['Bandeira'] = df_revendas['Bandeira'].str.title()
df_revendas['Municipio'] = df_revendas['Municipio'].str.title()
df_revendas = df_revendas.merge(df_municipio[['Municipio_id', 'Municipio']], on= 'Municipio')
df_revendas['Revenda_id'] = df_revendas.index +1
df_revendas = df_revendas[["Revenda_id", "Municipio_id", "Revenda", "CNPJ", "Bandeira"]]

#DATAFRAME PREÇOS
df_precos = df[["Revenda", "Produto", "Data da Coleta", "Valor de Venda", "Valor de Compra"]].drop_duplicates()
colunas_precos_tratadas = {
    "Data da Coleta" : "Data_coleta",
    "Valor de Venda" : "Valor_venda",
    "Valor de Compra" : "Valor_compra"
}
df_precos.rename(columns=colunas_precos_tratadas, inplace=True)
df_precos['Revenda'] = df_precos['Revenda'].str.title()
df_precos['Produto'] = df_precos['Produto'].str.title()
df_precos['Data_coleta'] = pd.to_datetime(df_precos['Data_coleta'], dayfirst=True).dt.strftime("%Y-%m-%d")
df_precos['Valor_venda'] = df_precos['Valor_venda'].str.replace(',', '.').astype(float) 
df_precos['Valor_compra'] = df_precos['Valor_compra'].fillna("Valor não informado")
df_precos = df_precos.merge(df_revendas[['Revenda_id', 'Revenda']], on= 'Revenda')
df_precos['Preco_id'] = df_precos.index +1
df_precos = df_precos[["Preco_id", "Revenda_id", "Produto", "Data_coleta", "Valor_venda", "Valor_compra"]]


#CARGA BANCO
user = os.getenv("USER_DB")
password = os.getenv("PASSWORD_DB")
host = os.getenv("HOST_DB")
port = os.getenv("PORT_DB")
dbname = "Combustivel_2025_02"

conexao_banco = f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(conexao_banco)

tabela_estados = "estados"
tabela_municipio = "municipio"
tabela_revendas = "revendas"
tabela_precos = "precos"
if_exists = 'replace'

df_estados.to_sql(name=tabela_estados, con=engine, index=False, if_exists=if_exists, dtype={
    "Estado_id" : Integer,
    "Regiao_sigla" : VARCHAR(5),
    "Estado_sigla" : VARCHAR(5)

})

df_municipio.to_sql(name=tabela_municipio, con=engine, if_exists=if_exists, index=False, dtype={
    "Municipio_id" : Integer,
    "Municipio" : VARCHAR(100),
    "Estado_sigla" : VARCHAR(5)
})

df_revendas.to_sql(name=tabela_revendas, con=engine, index=False, if_exists=if_exists, dtype={
    "Revenda_id" : Integer,
    "Municipio_id" : Integer,
    "Revenda" :VARCHAR(100),
    "CNPJ" : VARCHAR(20),
    "Bandeira" : VARCHAR(50)
    
})

df_precos.to_sql(name=tabela_precos, con=engine, index=False, if_exists=if_exists, dtype={
    "Preco_id" : Integer,
    "Revenda_id" : Integer,
    "Produto" : VARCHAR(50),
    "Data_coleta" : Date,
    "Valor_venda" : Float,
    "Valor_compra" : VARCHAR(50)
})


print("Carga realizada!")