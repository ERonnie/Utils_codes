import os
import time
import pandas as pd

def salvar_arquivo(df: pd.DataFrame, 
                   nome_arquivo: str, 
                   caminho: str | None = None,
                   extensao: str = "csv",
                   **kwargs
                   ) -> None:
    #Parametros padrões
    DEFAULTS = {"csv":{"sep": ";", "decimal": ",", "encoding": "utf-8", "index": False},
                "excel":{"sheet_name": "BD_Python", "index": False}
                }
    #Evita quebra por esse de digitação
    extensao = extensao.lower().strip()
    #Define o caminho
    try:
        pasta = caminho if caminho else os.getcwd()
    except Exception as e:
        print(f"Erro ao definir o caminho do arquivo {nome_arquivo}: {e}")
        return
    #Salva o arquivo na extensão escolhida pelo usuário
    match extensao:
        case "csv":
            arquivo = os.path.join(pasta, f"{nome_arquivo}.csv")
            params = {**DEFAULTS["csv"], **kwargs}
            try:
                print(f"Salvando arquivo: {nome_arquivo}")
                inicio = time.perf_counter()
                df.to_csv(arquivo, **params)
                fim = time.perf_counter() 
                arquivo = os.path.abspath(arquivo)
                print(f"Arquivo salvo em {arquivo}\nArquivo {nome_arquivo} salvo em {fim - inicio:.2f} segundos.")
            except Exception as e:
                print(f"Erro ao salvar o arquivo {nome_arquivo}: {e}")
        case "excel":
            arquivo = os.path.join(pasta, f"{nome_arquivo}.xlsx")
            params = {**DEFAULTS["excel"], **kwargs}
            try:
                print(f"Salvando arquivo: {nome_arquivo}")
                inicio = time.perf_counter()
                df.to_excel(arquivo, **params)
                fim = time.perf_counter() 
                arquivo = os.path.abspath(arquivo)
                print(f"Arquivo salvo em {arquivo}\nArquivo {nome_arquivo} salvo em {fim - inicio:.2f} segundos.")
            except Exception as e:
                print(f"Erro ao salvar o arquivo {nome_arquivo}: {e}")
        case _:
            raise ValueError(f"Extensão de arquivo '{extensao}' não suportada.")


def carregar_arquivo(caminho: str, **kwargs) -> pd.DataFrame:
    print("Iniciando o carregamento do arquivo")
    extensao = os.path.splitext(caminho)[1].lower()
    # parâmetros padrão
    defaults = {"csv": {"sep": ";", "decimal": ",", "encoding": "utf-8"},
                "excel": {"engine": None, "decimal": ",", "thousands": "."},
                "xlsb": {"engine": "pyxlsb", "decimal": ",", "thousands": "."}
                }
    
    inicio = time.time()

    match extensao:
        case ".csv":
            print("Extensão .csv detectada.")
            params = {**defaults["csv"], **kwargs}
            try:
                print("Lendo o arquivo csv em UTF-8")
                df = pd.read_csv(caminho, **params)
            except UnicodeDecodeError:
                print("Não foi possível ler em UTF-8, tentando latin1")
                params["encoding"] = "latin1"
                df = pd.read_csv(caminho, **params) 
        case ".xlsx" | ".xls" | ".xlsm":
            print("Extensão excel detectada.")
            params = {**defaults["excel"], **kwargs}
            df = pd.read_excel(caminho, **params)
        case ".xlsb":
            print("Extensâo .xlsb detectada.")
            params = {**defaults["xlsb"], **kwargs}
            df = pd.read_excel(caminho, **params)
        case _:
            raise ValueError(f"Extensão de arquivo '{extensao}' não suportada.")
    
    fim = time.time()
    df.columns = df.columns.str.strip()

    print(f"Arquivo {os.path.basename(caminho)} carregado em {fim - inicio:.2f} segundos.")
    return df