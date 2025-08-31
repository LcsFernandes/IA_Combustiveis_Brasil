from bs4 import BeautifulSoup
import requests
import zipfile
import os
import time
import tempfile
import polars as pl
import io
import unicodedata
import re

def limpar_colunas(colunas):
    colunas_limpas = []

    for col in colunas:
        col = col.strip().replace("\ufeff", "")
        col = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('utf-8')        
        col = col.lower().replace("-", "")
        col = re.sub(r'\s+', '_', col)

        colunas_limpas.append(col)
    return colunas_limpas



def baixar_relatorios_combustiveis_automotivos(url, path, numero_tentativas=5):
    try:

        response = requests.get(url)
        response.raise_for_status()
    except:
        raise Exception(f"Erro ao extrair dadaos da url: {url}")

    data = BeautifulSoup(response.text)

    combustiveis_automotivos = data.find("h3", string="Combustíveis automotivos")

    links = combustiveis_automotivos.find_next("ul")

    for link in links.find_all("a"):
                  
        for tentativa in range(1, numero_tentativas + 1):
            if link["href"].endswith(".csv"): 
                try:
                    suffixo = link["href"].split("/")[-1].split("-")[1] + "_" + link["href"].split("/")[-1].split("-")[2]
                    suffixo = suffixo.replace(".csv", ".parquet")
                    filename = "preco_combustivel_" + suffixo
                    
                    with requests.get(link["href"], stream=True, timeout=30) as response:
                        response.raise_for_status()

                        content = b''.join(response.iter_content(chunk_size=8192))
                        df = pl.read_csv(io.BytesIO(content), separator=";", encoding="utf8-lossy", ignore_errors=True)

                        df.columns = limpar_colunas(df.columns)

                        df.write_parquet(os.path.join(path, filename.replace(".csv", ".parquet")))
                                    
                    print(f"Arquivo {filename} extraído com sucesso!")
                    break    
                except:
                    print(f"nao foi possivel baixar o arquivo {filename} na {tentativa} tentativa")
                    time.sleep(5)

            elif link["href"].endswith(".zip"):
                try:
                    suffixo = link["href"].split("/")[-1].split("-")[1] + "_" + link["href"].split("/")[-1].split("-")[2]
                    suffixo = suffixo.replace(".zip", ".parquet")
                    filename = "preco_combustivel_" + suffixo

                    with requests.get(link["href"], stream=True, timeout=30) as response:
                        response.raise_for_status()
                        
                        with tempfile.NamedTemporaryFile(mode='w+b', suffix='.zip', delete=False) as tmp_file:
                            tmp_path = tmp_file.name
                            
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    tmp_file.write(chunk)                            
                            tmp_file.flush()
                            
                            with zipfile.ZipFile(tmp_path, 'r') as zip_ref:
                        
                                csv_files = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
                                
                                if not csv_files:
                                    raise ValueError("Nenhum arquivo CSV encontrado no ZIP")
                                
                                csv_filename = csv_files[0]
                                
                                with zip_ref.open(csv_filename) as source:
                                    csv_bytes = source.read()
                                    df = pl.read_csv(io.BytesIO(csv_bytes), separator=";", encoding="utf8-lossy", ignore_errors=True)

                                    df.columns = limpar_colunas(df.columns)

                                    df.write_parquet(os.path.join(path, filename))
                                
                                print(f"Arquivo {filename} extraído com sucesso!")
                    
                    try:
                        os.unlink(tmp_path)
                    except Exception as e:
                        pass
                        
                    break  
                    
                except Exception as e:
                    if 'tmp_path' in locals() and os.path.exists(tmp_path):
                        try:
                            os.unlink(tmp_path)
                        except Exception as e:
                            pass
                    
                    if tentativa == numero_tentativas:
                        print(f"FALHA ao processar {filename} após {numero_tentativas} tentativas")
                    time.sleep(5)  