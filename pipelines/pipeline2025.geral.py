import pandas as pd 
import os 
import unicodedata
from dotenv import load_dotenv
from sqlalchemy import create_engine, Integer, VARCHAR, Float, Date

load_dotenv()
caminho_arquivo = os.getenv("CSV_PATH_GERAL")
df = pd.read_csv(caminho_arquivo, delimiter=';', encoding='latin1', low_memory=False)

def normalizar_texto(texto):
    if pd.isna(texto):
        return texto
    return (unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8').title())

#DATAFRAME ESTADO/REGIAO
df_estados = df[['ï»¿Regiao - Sigla', 'Estado - Sigla']].drop_duplicates()
colunas_tratadas_estados = {
    'ï»¿Regiao - Sigla' : 'Regiao_sigla',
    'Estado - Sigla' : 'Estado_sigla'
}
df_estados.rename(columns=colunas_tratadas_estados, inplace=True)
df_estados['Estado_id'] = df_estados.index +1
df_estados = df_estados[['Estado_id', 'Regiao_sigla', 'Estado_sigla']]

#DATAFRAME MUNICIPIO
df_municipio = df[['Municipio', 'Estado - Sigla' ]].drop_duplicates()
df_municipio.rename(columns={'Estado - Sigla' : 'Estado_sigla'}, inplace=True)
df_municipio['Municipio'] = df_municipio['Municipio'].apply(normalizar_texto)
df_municipio['Municipio_id'] = df_municipio.index +1
df_municipio = df_municipio.merge(df_estados[['Estado_id', 'Estado_sigla']], on= 'Estado_sigla')
df_municipio = df_municipio[['Municipio_id','Estado_id', 'Municipio', 'Estado_sigla']]

#DATAFRAME REVENDAS
df_revendas = df[['Revenda', 'CNPJ da Revenda', 'Bandeira', 'Municipio']].drop_duplicates()
df_revendas.rename(columns={'CNPJ da Revenda' : 'CNPJ'}, inplace=True)
df_revendas['Municipio'] = df_revendas['Municipio'].apply(normalizar_texto)
df_revendas['CNPJ'] = df_revendas['CNPJ'].astype(str)
df_revendas['Revenda'] = df_revendas['Revenda'].str.title()
df_revendas['Bandeira'] = df_revendas['Bandeira'].str.title()
df_revendas['Revenda_id'] = df_revendas.index +1
df_revendas = df_revendas.merge(df_municipio[['Municipio_id', 'Municipio']], on= 'Municipio')
df_revendas = df_revendas[['Revenda_id', 'Municipio_id', 'Revenda', 'CNPJ', 'Bandeira','Municipio']]

#DATAFRAME PREÇO
df_precos = df[['Produto', 'Data da Coleta', 'Valor de Venda', 'Valor de Compra', 
                'Unidade de Medida', 'Municipio', 'Revenda']].drop_duplicates()
colunas_tratadas_precos = {
    'Data da Coleta' : 'Data_coleta',
    'Valor de Venda' : 'Valor_venda',
    'Valor de Compra' : 'Valor_compra',
    'Unidade de Medida' : 'Unidade_medida'
}

def status_preco(preco):
    if pd.isna(preco):
        return "Preço não identificado"
    elif preco < 6.0:
        return "Preço abaixo"
    elif preco >= 6.0 and preco <= 6.50:
        return "Preço normal"
    else:
        return "Preço alto"    

df_precos.rename(columns=colunas_tratadas_precos, inplace=True)
df_precos['Municipio'] = df_precos['Municipio'].apply(normalizar_texto)
df_precos['Data_coleta'] = pd.to_datetime(df_precos['Data_coleta'], dayfirst=True).dt.strftime("%Y-%m-%d")
df_precos['Valor_venda'] = df_precos['Valor_venda'].astype(str).str.replace(',', '.', regex=True)
df_precos['Valor_venda'] = pd.to_numeric(df_precos['Valor_venda'], errors='coerce')
df_precos['Status_preco'] = df_precos['Valor_venda'].apply(status_preco)
df_precos['Valor_compra'] = df_precos['Valor_compra'].fillna("Valor não informado")
df_precos['Produto'] = df_precos['Produto'].str.title()
df_precos['Revenda'] = df_precos['Revenda'].str.title()
df_precos['Preco_id'] = df_precos.index +1
df_precos = df_precos.merge(df_municipio[['Municipio_id', 'Municipio']], on= 'Municipio')
df_precos = df_precos.merge(df_revendas[['Revenda_id', 'Revenda']], on= 'Revenda')
df_precos = df_precos[['Preco_id', 'Municipio_id', 'Revenda_id', 'Produto', 'Data_coleta',
                        'Valor_venda', 'Status_preco', 'Valor_compra', 'Unidade_medida', 'Municipio', 'Revenda']]

#DATAFRAME ENDEREÇO
df_endereco = df[['Nome da Rua', 'Numero Rua', 'Complemento', 'Bairro',	
                  'Cep','Municipio']].drop_duplicates()
colunas_tratadas_endereco = {
    'Nome da Rua' : 'Nome_rua',
    'Numero Rua' : 'Numero_rua',
    'Cep' : 'CEP'
}
df_endereco.rename(columns=colunas_tratadas_endereco, inplace=True)
df_endereco['Municipio'] = df_endereco['Municipio'].apply(normalizar_texto)
df_endereco['Nome_rua'] = df_endereco['Nome_rua'].str.title()
df_endereco['Complemento'] = df_endereco['Complemento'].str.title()
df_endereco['Complemento'] = df_endereco['Complemento'].fillna("Complemento não informado")
df_endereco['Bairro'] = df_endereco['Bairro'].str.title()
df_endereco['CEP'] = df_endereco['CEP'].astype(str)
df_endereco['Endereco_id'] = df_endereco.index +1
df_endereco = df_endereco.merge(df_municipio[['Municipio_id', 'Municipio']], on= 'Municipio')
df_endereco = df_endereco[['Endereco_id', 'Municipio_id', 'Nome_rua', 'Numero_rua',
                            'Complemento', 'Bairro', 'CEP', 'Municipio']]

#CARGA BANCO
user = os.getenv("USER_DB")
password = os.getenv("PASSWORD_DB")
host = os.getenv("HOST_DB")
port = os.getenv("PORT_DB")
dbname = "Combustivel_2025_geral"

conexao_banco = f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(conexao_banco)

tabela_estados = 'estado'
tabela_municipio = 'municipio'
tabela_revendas = 'revendas'
tabela_precos = 'precos'
tabela_endereco = 'endereco'

df_estados.to_sql(name= tabela_estados, con= engine, index= False, if_exists='replace', dtype={
    "Estado_id" : Integer,
    "Regiao_sigla" : VARCHAR(5),
    "Estado_sigla" : VARCHAR(5)
})

df_municipio.to_sql(name= tabela_municipio, con= engine, index= False, if_exists= 'replace', dtype={
    "Municipio_id" : Integer,
    "Estado_id" : Integer,
    "Municipio" : VARCHAR(100),
    "Estado_sigla" : VARCHAR(5)
})

df_revendas.to_sql(name= tabela_revendas, con= engine, index= False, if_exists= 'replace', dtype={
    "Revenda_id" : Integer,
    "Municipio_id" : Integer,
    "Revenda" : VARCHAR(100),
    "CNPJ" : VARCHAR(50),
    "Bandeira" : VARCHAR(50),
    "Municipio" : VARCHAR(100)
})

df_precos.to_sql(name= tabela_precos, con= engine, index= False, if_exists= 'replace', dtype={
    "Preco_id" : Integer,
    "Municipio_id" : Integer,
    "Revenda_id" : Integer,
    "Produto" : VARCHAR(50),
    "Data_coleta" : Date,
    "Valor_venda" : Float,
    "Status_preco" : VARCHAR(30),
    "Valor_compra" : VARCHAR(30),
    "Unidade_medida" : VARCHAR(30),
    "Municipio" : VARCHAR(100),
    "Revenda" : VARCHAR(100)
})

df_endereco.to_sql(name= tabela_endereco, con= engine, index= False, if_exists= 'replace', dtype={
    "Endereco_id" : Integer,
    "Municipio_id" : Integer, 
    "Nome_rua" : VARCHAR(100),
    "Numero_rua" : VARCHAR(20),
    "Complemento" : VARCHAR(255),
    "Bairro" : VARCHAR(50),
    "CEP" : VARCHAR(50),
    "Municipio" : VARCHAR(100)  
})

print("Carga realizada!")

