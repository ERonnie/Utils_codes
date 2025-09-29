import os
import time
import pandas as pd


def salvar_csv(df: pd.DataFrame, 
               nome_arquivo: str, 
               caminho: str | None = None,
               separador: str = ";", 
               codificacao: str = "utf-8"
               ) -> None:
    
    #Define o caminho
    pasta = caminho if caminho else os.getcwd()
    arquivo = os.path.join(pasta, f"{nome_arquivo}.csv")
    #Salva o arquivo
    print(f"Salvando arquivo: {nome_arquivo}")
    inicio = time.perf_counter()

    df.to_csv(arquivo, sep=separador, encoding=codificacao, decimal=",", index=False)

    fim = time.perf_counter() 
    arquivo = os.path.abspath(arquivo)
    print(f"Arquivo salvo em {arquivo}\nArquivo {nome_arquivo} salvo em {fim - inicio:.2f} segundos.")