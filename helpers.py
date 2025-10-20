import os
import time
import warnings
import pandas as pd
import polars as pl
warnings.filterwarnings("ignore")

def salvar_arquivo(df: pd.DataFrame, 
                   nome_arquivo: str, 
                   caminho: str | None = None,
                   extensao: str = "csv",
                   **kwargs
                   ) -> None:
    #Parametros padrões
    DEFAULTS_PANDAS = {"csv":{"sep": ";", "decimal": ",", "encoding": "utf-8", "index": False},
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


def carregar_arquivo(caminho: str, engine: str = "pandas", **kwargs) -> pd.DataFrame:
    print("Iniciando o carregamento do arquivo")
    extensao = os.path.splitext(caminho)[1].lower()
    # parâmetros padrão
    if engine.lower() == "pandas":
        DEFAULTS_PANDAS = {
            "csv": {"sep": ";", "decimal": ",", "encoding": "utf-8"},
            "excel": {"engine": None, "decimal": ",", "thousands": "."},
            "xlsb": {"engine": "pyxlsb", "decimal": ",", "thousands": "."}
        }
        print("Usando Pandas como engine de leitura.")
        inicio = time.time()

        match extensao:
            case ".csv":
                print("Extensão .csv detectada.")
                params = {**DEFAULTS_PANDAS["csv"], **kwargs}
                try:
                    print("Lendo o arquivo csv em UTF-8")
                    df = pd.read_csv(caminho, **params)
                except UnicodeDecodeError:
                    print("Não foi possível ler em UTF-8, tentando latin1")
                    params["encoding"] = "latin1"
                    df = pd.read_csv(caminho, **params) 
            case ".xlsx" | ".xls" | ".xlsm":
                print("Extensão excel detectada.")
                params = {**DEFAULTS_PANDAS["excel"], **kwargs}
                df = pd.read_excel(caminho, **params)
            case ".xlsb":
                print("Extensâo .xlsb detectada.")
                params = {**DEFAULTS_PANDAS["xlsb"], **kwargs}
                df = pd.read_excel(caminho, **params)
            case _:
                raise ValueError(f"Extensão de arquivo '{extensao}' não suportada.")
        
        fim = time.time()
        df.columns = df.columns.str.strip()

        print(f"Arquivo {os.path.basename(caminho)} carregado em {fim - inicio:.2f} segundos.")
        return df
    else:
        DEFAULTS_POLARS = {
            "csv": {"separator": ";", "decimal_comma": True, "encoding": "utf-8"},
            "excel": {"read_options": {"header_row": 0}},
        }
        print("Usando Polars como engine de leitura.")
        inicio = time.time()

        match extensao:
            case ".csv":
                print("Extensão .csv detectada.")
                params = {**DEFAULTS_POLARS["csv"], **kwargs}
                try:
                    print("Lendo o arquivo csv em UTF-8")
                    df = pl.read_csv(caminho, **params)
                    df = df.to_pandas()
                except UnicodeDecodeError:
                    print("Não foi possível ler em UTF-8, tentando latin1")
                    params["encoding"] = "latin1"
                    df = pl.read_csv(caminho, **params) 
                    df = df.to_pandas()
            case ".xlsx" | ".xls" | ".xlsm" | ".xlsb":
                print("Extensão excel detectada.")
                params = {**DEFAULTS_POLARS["excel"], **kwargs}
                df = pl.read_excel(caminho, **params)
                df = df.to_pandas()
            case _:
                raise ValueError(f"Extensão de arquivo '{extensao}' não suportada.")
        
        fim = time.time()
        df.columns = df.columns.str.strip()

        print(f"Arquivo {os.path.basename(caminho)} carregado em {fim - inicio:.2f} segundos.")
        return df


def ajustar_data(df: pd.DataFrame, coluna: str, reportar_erros: bool = True) -> pd.DataFrame:
    
    if coluna not in df.columns:
        raise KeyError(f"A coluna '{coluna}' não existe no DataFrame.")
    
    df_ajustado = df
    serie = df[coluna]

    # Se já for datetime, só formata
    if pd.api.types.is_datetime64_any_dtype(serie):
        df_ajustado[coluna] = serie.dt.strftime("%Y-%m-%d")
        return df_ajustado
    # Caso for string
    df_ajustado[coluna] = df[coluna].astype(str).str.strip()
    mask_ano_mes = df_ajustado[coluna].str.match(r"^\d{4}[-/]\d{2}$", na=False)
    df_ajustado.loc[mask_ano_mes, coluna] = df.loc[mask_ano_mes, coluna] + "-01"
    # df_ajustado[coluna] = pd.to_datetime(serie, errors='coerce', dayfirst=False)

    convertida = pd.to_datetime(serie, errors="coerce", dayfirst=False)
    invalidos = serie[convertida.isna() & serie.notna()]

    df_ajustado[coluna] = convertida

    if reportar_erros and not invalidos.empty:
        print(f"⚠️ Total de {len(invalidos)} valores inválidos encontrados na coluna '{coluna}':")
        print(invalidos.to_list())

    return df_ajustado

def ajustar_colunas(
        df: pd.DataFrame,
        ajustar_para: str = "Maisculas"
):
    columns_strings = df.select_dtypes(include=["object"]).columns
    for col in columns_strings:
        if ajustar_para == "Maisculas":
            df[col]= df[col].str.upper()
        elif ajustar_para == "Minusculas":
            df[col] = df[col].str.lower()
    return df