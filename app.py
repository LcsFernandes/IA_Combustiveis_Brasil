import duckdb
import pandas as pd
import polars as pl
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain_community.llms import Ollama
import requests
import time
import re
import os
from dotenv import load_dotenv

load_dotenv()

URL_DATA_SILVER = os.getenv("URL_DATA_SILVER")

if not URL_DATA_SILVER:
    raise Exception("URL_DATA nao encontrado")

def check_ollama():
    try:
        response = requests.get("http://localhost:11434/api/tags", timeout=10)
        return response.status_code == 200
    except:
        return False

def setup_ollama_llm():
    print("Conectando ao Ollama")
    
    for i in range(5):
        if check_ollama():
            break
        time.sleep(2)
    
    if not check_ollama():
        print("Ollama não está respondendo")
        return None
    
    try:
        response = requests.get("http://localhost:11434/api/tags", timeout=10)
        models = response.json().get('models', [])
        
        if not models:
            print("Nenhum modelo encontrado")
            return None
        
        print("Modelos disponíveis:")
        for model in models:
            print(f"   - {model['name']}")
        
        model_name = "phi3"
        print(f"Usando modelo: {model_name}")
        
        llm = Ollama(
            model=model_name,
            temperature=0.1,
            num_thread=4,
            timeout=120
        )
        
        print("LLM configurado com sucesso!")
        return llm
        
    except Exception as e:
        print(f"Erro ao configurar LLM: {e}")
        return None


def load_data():
    url = URL_DATA_SILVER
    
    print("Carregando dados para o modelo")
    df_pl = pl.read_parquet(url)
    con = duckdb.connect(database=':memory:')
    con.register('combustiveis', df_pl.to_pandas())
    
    df_meta = pd.DataFrame({
        "colunas": [c[0] for c in con.execute("DESCRIBE combustiveis").fetchall()]
    })
    
    print(f"Dados carregados")
    return con, df_meta

def extract_sql_from_response(response):
    
    if re.match(r'^\s*(SELECT|WITH|INSERT|UPDATE|DELETE|CREATE)', response, re.IGNORECASE):
        return response
    
    
    sql_blocks = re.findall(r'```sql\s*(.*?)\s*```', response, re.DOTALL | re.IGNORECASE)
    if sql_blocks:
        return sql_blocks[0].strip()
    
    
    sql_pattern = r'(SELECT\s+.+?FROM\s+.+?(?:WHERE\s+.+?)?(?:GROUP BY\s+.+?)?(?:ORDER BY\s+.+?)?(?:LIMIT\s+\d+)?)'
    matches = re.findall(sql_pattern, response, re.IGNORECASE | re.DOTALL)
    if matches:
        return matches[0].strip()
    
    
    return response

def clean_sql_query(sql_query):
    """Limpa a query SQL"""
    sql_query = sql_query.strip()
    
    
    if sql_query.endswith(';'):
        sql_query = sql_query[:-1]
    
    
    sql_query = re.sub(r'--.*?$', '', sql_query, flags=re.MULTILINE)
    sql_query = re.sub(r'/\*.*?\*/', '', sql_query, flags=re.DOTALL)
    
    return sql_query.strip()



def validate_and_fix_query(sql_query, valid_columns):
    
    sql_query = extract_sql_from_response(sql_query)
    sql_query = clean_sql_query(sql_query)
    
    
    function_replacements = {
        r"DATE\('now\(\)',\s*'\-?\d+\s+days?'\)": "CURRENT_DATE",
        r"DATE\('now\(\)'\)": "CURRENT_DATE",
        r"NOW\(\)": "CURRENT_TIMESTAMP",
        r"DATE_SUB\([^)]+\)": "CURRENT_DATE",
    }
    
    for pattern, replacement in function_replacements.items():
        sql_query = re.sub(pattern, replacement, sql_query, flags=re.IGNORECASE)
    
    
    sql_query = re.sub(
        r'data_da_coleta\s*>=\s*CURRENT_DATE', 
        "data_da_coleta >= CAST(CURRENT_DATE AS TIMESTAMP)", 
        sql_query, 
        flags=re.IGNORECASE
    )
    
    
    sql_query = re.sub(r"produto\s*=\s*'Gasolina'", "produto = 'GASOLINA'", sql_query, flags=re.IGNORECASE)
    sql_query = re.sub(r"produto\s*=\s*'Diesel'", "produto = 'DIESEL'", sql_query, flags=re.IGNORECASE)
    sql_query = re.sub(r"produto\s*=\s*'Etanol'", "produto = 'ETANOL'", sql_query, flags=re.IGNORECASE)
    
    return sql_query

def extract_parameters(pergunta):
    params = {
        'ano': None,
        'mes': None, 
        'estado': None,
        'regiao': None,
        'produto': None,
    }
    
    
    year_match = re.search(r'(20\d{2})', pergunta)
    if year_match:
        params['ano'] = year_match.group(1)
    
    
    produtos = ['gasolina', 'diesel', 'etanol', 'gnv', 'gasolina aditivada']
    for produto in produtos:
        if produto in pergunta.lower():
            params['produto'] = produto.upper()
            break
    
    return params

def corrigir_query(query_errada, pergunta, con, df_meta, llm):
    colunas_reais = df_meta['colunas'].tolist()
    params = extract_parameters(pergunta)
    
    prompt_correcao = f"""
    A query SQL DuckDB falhou: {query_errada}
    
    COLUNAS VÁLIDAS: {', '.join(colunas_reais)}
    PARÂMETROS IDENTIFICADOS: {params}
    
    REGRAS ESTRITAS:
    1. Use EXTRACT(YEAR FROM data_da_coleta) = X para anos
    2. Para gasolina: WHERE produto = 'GASOLINA'
    3. Retorne APENAS a query SQL sem explicações
    4. Use sintaxe DuckDB válida
    
    Corrija a query para: '{pergunta}'
    """
    
    try:
        query_corrigida = llm.invoke(prompt_correcao)
        query_corrigida = extract_sql_from_response(query_corrigida)
        query_corrigida = clean_sql_query(query_corrigida)
        query_corrigida = validate_and_fix_query(query_corrigida, colunas_reais)
        
        #print(f"Query corrigida: {query_corrigida}")
        
        resultado = con.execute(query_corrigida).fetchall()
        return formatar_resultado(resultado)
        
    except Exception as e:
        return f"Falha na correção: {str(e)}"

def formatar_resultado(resultado):
    
    if not resultado:
        return "Nenhum resultado encontrado."
    
    if isinstance(resultado, list) and resultado:
        if isinstance(resultado[0], tuple):
            formatted = []
            for item in resultado:
                if len(item) == 2:
                    formatted.append(f"'{item[0]}'={item[1]}")
                else:
                    formatted.append(str(item))
            return ", ".join(formatted)
    
    return str(resultado)

def responder_com_llm(pergunta, con, df_meta, llm):
    try:
        print("Processando com LLM")
        
        colunas_reais = df_meta['colunas'].tolist()
        params = extract_parameters(pergunta)

        prompt = f"""
        Gere APENAS uma query SQL DuckDB válida para: '{pergunta}'

        TABELA: combustiveis
        COLUNAS: {', '.join(colunas_reais)}
        PARÂMETROS IDENTIFICADOS: {params}

        REGRAS ABSOLUTAS:
        1. Retorne SOMENTE a query SQL
        2. Sem explicações, sem texto adicional
        3. Use apenas estas colunas
        4. Para extrair ano: EXTRACT(YEAR FROM data_da_coleta) = X
        5. Para gasolina: WHERE produto = 'GASOLINA'
        6. Quando usar AVG(), COUNT(), SUM() com outras colunas, ADICIONE GROUP BY

        EXEMPLOS CORRETOS:
        - "preço em 2014": SELECT AVG(valor_de_venda) FROM combustiveis WHERE produto = 'GASOLINA' AND EXTRACT(YEAR FROM data_da_coleta) = 2014
        - "preço em São Paulo": SELECT AVG(valor_de_venda) FROM combustiveis WHERE produto = 'GASOLINA' AND estado_sigla = 'SP'

        EXEMPLO DOS DADOS:
        regiao_sigla ┆ estado_sigla ┆ municipio ┆ revenda         ┆ cnpj_da_revenda ┆ bairro         ┆ produto  ┆ data_da_coleta ┆ valor_de_venda ┆ valor_de_compra ┆ unidade_de_med ┆ bandeira       ┆ regiao  ││ 
        │ SE           ┆ SP           ┆ GUARULHOS ┆ AUTO POSTO      ┆ 49051667000102  ┆ BONSUCESSO     ┆ GASOLINA ┆ 2004-05-11     ┆ 1.967          ┆ 1.6623          ┆ R$ / litro     ┆ PETROBRAS      ┆ Sudeste ││         

        Gere a query SQL:
        """
        
        resposta = llm.invoke(prompt)
        sql_query = extract_sql_from_response(resposta)
        sql_query = clean_sql_query(sql_query)
        sql_query = validate_and_fix_query(sql_query, colunas_reais)
        
        #print(f"Query gerada: {sql_query}")
        
        try:
            resultado = con.execute(sql_query).fetchall()
            return formatar_resultado(resultado)
            
        except Exception as e:
            print(f"Query falhou: {e}")
            return corrigir_query(sql_query, pergunta, con, df_meta, llm)
            
    except Exception as e:
        return f"Erro no processamento: {str(e)}"


def main():
    print("Iniciando sistema de consulta com Ollama")
    
    llm = setup_ollama_llm()
    if not llm:
        print("Não foi possível inicializar o LLM. Encerrando.")
        return
    
    con, df_meta = load_data()
    
    print("\n" + "="*60)
    print("IA ATIVADA - Sistema pronto para consultas")
    print("="*60)
    print("- Digite 'sair' para encerrar")
    print("="*60)
    
    while True:
        try:
            pergunta = input("\n Sua pergunta: ").strip()
            if pergunta.lower() in ["sair", "exit", "quit"]:
                break
            if not pergunta:
                continue
                
            resposta = responder_com_llm(pergunta, con, df_meta, llm)
            
            print(f"\n resposta: {resposta}")
            
        except KeyboardInterrupt:
            print("\n Encerrando")
            break
        except Exception as e:
            print(f"Erro inesperado: {e}")

if __name__ == "__main__":
    main()