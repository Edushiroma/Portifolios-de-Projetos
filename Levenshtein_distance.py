import pandas as pd
import os
import re
from datetime import datetime
from rapidfuzz import fuzz # Utiliza Levenshtein distance para comparar strings
from unidecode import unidecode
from sqlalchemy import create_engine, inspect, text

# Funcao para encontrar nomes similares
def identificar_nomes_similares(df, coluna, limiar=80): #Percentual de colisao
    nomes_unicos = df[coluna].dropna().unique()
    similares = {}

    for i, nome in enumerate(nomes_unicos):
        for outro_nome in nomes_unicos[i+1:]:
            score = fuzz.ratio(nome, outro_nome) # Comparar similaridade entre os nomes
            if score >= limiar:
                if nome not in similares: 
                    similares[nome] = []
                similares[nome].append((outro_nome, score))
    return similares



# Funcao para conexao e carga no banco de dados
def carregar_dados_no_banco( mes_ano, conn_str):
    tabela = 'Inserir aqui sua tabela ou planilha para processamento'
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={conn_str}')
 
    query = f"SELECT DISTINCT NOME FROM {tabela}" #Ajustar aqui a sua tabela
    df = pd.read_sql(query, engine)
 
    similares = identificar_nomes_similares(df, 'NOME')
 
    df_similares = pd.DataFrame([(nome, similar, score) for nome, lista_similares in similares.items() for similar, score in lista_similares],
                                columns=['Nome', 'Nome Similar', 'Similaridade'])
    
    caminho_similares = f'c:/similares_{mes_ano}.csv'
    df_similares.to_csv(caminho_similares, sep=';', index=False, encoding='utf-8-sig')
    
    print(f"Similares salvos em: {caminho_similares}")    

    engine.dispose()



def main():
    ### Processa dados baseados na data de execucao, sempre ira rodar o mes anterior ####
    data_execucao = datetime.now()
    ano = data_execucao.year
    if data_execucao.month == 1:
        mes_anterior = 12
        ano -= 1
    else:
        mes_anterior = data_execucao.month - 1
    mes_anterior = str(mes_anterior).zfill(2)

    mes_ano = f"{mes_anterior}_{ano}"

    # Conexão com o SQL Server
    conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=;'
        'DATABASE=;'
        'UID=;'
        'PWD='
    )

    # Carregar dados no banco de dados
    tabela_nome = f'PESSOA'
    carregar_dados_no_banco(mes_ano,conn_str)
    

if __name__ == "__main__":
    main()
