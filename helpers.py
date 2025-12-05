import os
import time
import warnings
import pandas as pd
import polars as pl
warnings.filterwarnings("ignore")

def salvar_arquivo(
    df: pd.DataFrame, 
    nome_arquivo: str, 
    caminho: str | None = None,
    extensao: str = "csv",
    **kwargs
) -> None:
    """

    Função desenvolvida para salvar arquivos de forma mais rápida e padrão, evitando problemas de
    padronização nas saidas de arquivos e repetição em codigos.
    
    :param df: Dataframe que será salvo
    :param nome_arquivo: Nome para saida de arquivo
    :param caminho: Local de saída do arquivo 
    :param extensao: Qual extensão será salva (csv, xlsx, etc)
    :param kwargs: Pode ser passado qualquer tipo de **kwargs padrão Pandas

    """

    #Parametros padrões
    DEFAULTS = {
        "csv":{"sep": ";", "decimal": ",", "encoding": "utf-8", "index": False},
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

def carregar_arquivo(
    caminho: str, 
    engine: str = "pandas", 
    limpar = False, 
    uppercase = False, 
    **kwargs
) -> pd.DataFrame:
    """

    Função carrega arquivos .csv, .xlsx(e suas variaveis), tratando ele dependendo da sua necessidade
    e retorna um DataFrame pronto para fazer qualquer processo de analise sem precisar se preocupar
    com espaços em brancos, valores sem padronização.
    
    :param caminho: Local do arquivo a ser carregado
    :type caminho: str
    :param engine: Qual biblioteca será utilizada para carregar o arquivo, polars será mais rápido 
    e melhor para ler arquivos pesados e com muitas informações.
    :type engine: str
    :param limpar: Se True remove espaços em brancos de todos o dados do DataFrame. 
    obs: somente em colunas Object
    :param uppercase: Se True transforma dados Object em caixa alta (uppercase)
    :param kwargs: Aceita qualquer **kwargs de leitura, deve ser respeitado a syntax da biblioteca
    utilizada no engine.

    :return: Tratado em formato Pandas independente de qual engine foi processada
    :rtype: DataFrame

    """

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
        # Remove espaços em branco nos nomes da colunas
        df.columns = df.columns.str.strip()
        # Se True remove espaços em branco nas linhas
        if limpar:
            for col in df.columns:
                if df[col].dtype == 'object' or pd.api.types.is_string_dtype(df[col]):
                    df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        # Se True torna tudo uppercase
        if uppercase:
            for col in df.columns:
                if df[col].dtype == 'object' or pd.api.types.is_string_dtype(df[col]):
                    df[col] = df[col].apply(lambda x: x.upper() if isinstance(x, str) else x)
                    
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
        if limpar:
            for col in df.columns:
                if df[col].dtype == 'object' or pd.api.types.is_string_dtype(df[col]):
                    df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
            
        print(f"Arquivo {os.path.basename(caminho)} carregado em {fim - inicio:.2f} segundos.")
        return df

def ajustar_data(df: pd.DataFrame, coluna: str, reportar_erros: bool = True) -> pd.DataFrame:
    """

    Ajusta a coluna de data do DataFrame para sair no padrão yyyy-mm-dd
    
    :param df: DataFrame a ser ajustado
    :type df: pd.DataFrame
    :param coluna: Coluna de Data a ser ajustada
    :type coluna: str
    :param reportar_erros: Reporta valores que não se encaixam no filtro de data, ou seja valores
    que não é possivel tranformar em data
    :type reportar_erros: bool
    :return: DataFrame com colunas de Data ajustada
    :rtype: DataFrame

    """
    
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

    convertida = pd.to_datetime(serie, errors="coerce", dayfirst=False)
    invalidos = serie[convertida.isna() & serie.notna()]

    df_ajustado[coluna] = convertida

    if reportar_erros and not invalidos.empty:
        print(f"⚠️ Total de {len(invalidos)} valores inválidos encontrados na coluna '{coluna}':")
        print(invalidos.to_list())

    return df_ajustado

def ajustar_colunas(df: pd.DataFrame, ajustar_para: str = "maisculas"):
    """

    Caso precise transformar todo o DataFrame em maiusculo ou minusculo, somente colunas que seja
    Object

    :param df: DataFrame para ser ajustado
    :type df: pd.DataFrame
    :param ajustar_para: Determine se será transformado tudo em minusculo ou tudo em maisculo
    :type ajustar_para: str
    :return: DataFrame com colunas ajustada
    :rtype: DataFrame

    """

    ajustar_para = ajustar_para.lower().strip()
    columns_strings = df.select_dtypes(include=["object"]).columns
    for col in columns_strings:
        if ajustar_para == "maisculas":
            df[col]= df[col].str.upper()
        elif ajustar_para == "minusculas":
            df[col] = df[col].str.lower()

    return df