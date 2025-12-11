import pandas as pd
import os

class DesdobradorProporcional:
    def __init__(self, df_origem, df_destino, chaves_origem, chaves_destino, coluna_valor):
        """
        Classe para realizar desdobramentos proporcionais entre dois níveis hierárquicos.

        Parameters:
            df_origem (pd.DataFrame): DataFrame com menor nível de detalhe.
            df_destino (pd.DataFrame): DataFrame com maior nível de detalhe (alvo).
            chaves_origem (list): Lista de chaves do nível origem.
            chaves_destino (list): Lista de chaves do nível destino.
            coluna_valor (str): Nome da coluna a ser desdobrada.
        """

        self.df_origem = df_origem.copy()
        self.df_destino = df_destino.copy()
        self.chaves_origem = chaves_origem
        self.chaves_destino = chaves_destino
        self.coluna_valor = coluna_valor

        # atributos gerados após execução
        self.df_ok = None
        self.df_erro = None

        # validações iniciais
        self._validar_chaves()

        # Atributo para armazenar a soma total da origem (útil para auditoria)
        self.soma_origem_total = self.df_origem[self.coluna_valor].sum()


    def _validar_chaves(self):
        """Valida se existem chaves comuns entre origem e destino."""
        self.chaves_comuns = [c for c in self.chaves_origem if c in self.chaves_destino]

        if not self.chaves_comuns:
            raise ValueError("Não existem chaves comuns entre origem e destino.")


    def desdobrar(self):
        """Executa o desdobramento proporcional."""

        df_origem, df_destino = self.df_origem, self.df_destino
        chaves_comuns = self.chaves_comuns
        coluna_valor = self.coluna_valor
        chaves_origem = self.chaves_origem

        # -------------------------------------------
        # 1) Identificar origem sem destino correspondente
        # -------------------------------------------
        df_origem_check = df_origem.merge(
            df_destino[chaves_comuns].drop_duplicates(),
            on=chaves_comuns,
            how="left",
            indicator=True
        )

        self.df_erro = (
            df_origem_check[df_origem_check["_merge"] == "left_only"]
            .drop(columns="_merge")
        )

        df_origem_valida = (
            df_origem_check[df_origem_check["_merge"] == "both"]
            .drop(columns="_merge")
        )

        soma_erro = self.df_erro[coluna_valor].sum()
        print(f"**Relatório de Desdobramento:**")
        print(f"Soma total de Origem: {self.soma_origem_total:,.2f}")
        print(f"Valor não distribuído (Erros): {soma_erro:,.2f}")

        # se nada sobrou, retorna vazio
        if df_origem_valida.empty:
            self.df_ok = pd.DataFrame()
            return self.df_ok, self.df_erro

        # -------------------------------------------
        # 2) Filtrar destino pelas chaves válidas
        # -------------------------------------------
        df_destino_ok = df_destino.merge(
            df_origem_valida[chaves_comuns].drop_duplicates(),
            on=chaves_comuns,
            how="inner"
        )

        # -------------------------------------------
        # 3) Agrupar e criar pesos
        # -------------------------------------------
        destino_agrupado = (
            df_destino_ok
            .groupby(chaves_comuns)[coluna_valor]
            .sum()
            .reset_index()
            .rename(columns={coluna_valor: "soma_destino"})
        )

        df_destino_ok = df_destino_ok.merge(destino_agrupado, on=chaves_comuns, how="left")

        # Cálculo do Peso: Adicionamos um tratamento para evitar a divisão por zero.
        # Se soma_destino for 0, definimos o peso como 0, pois o valor do destino também será 0
        # e a proporção de 0/0 é indeterminada, mas 0 * valor_origem = 0.
        df_destino_ok["peso"] = df_destino_ok[coluna_valor].mask(
            df_destino_ok["soma_destino"] != 0,
            df_destino_ok[coluna_valor] / df_destino_ok["soma_destino"]
        ).fillna(0) # Se a soma for 0 (NaN após a operação .mask), o peso é 0.

        df_destino_ok = df_destino_ok.merge(
            df_origem_valida[chaves_origem + [coluna_valor]],
            on=chaves_comuns,
            how="left",
            suffixes=("", "_origem")
        )

        df_destino_ok["valor_desdobrado"] = (
            df_destino_ok["peso"] * df_destino_ok[f"{coluna_valor}_origem"]
        )

        soma_desdobrado = df_destino_ok['valor_desdobrado'].sum()
        print(f"Valor desdobrado (OK): {soma_desdobrado:,.2f}")
        print(f"Diferença Total: {(self.soma_origem_total - soma_desdobrado - soma_erro):.2f} (Esperado 0.00)")


        df_destino_ok = df_destino_ok.drop(
            columns=["soma_destino", "peso", f"{coluna_valor}_origem"]
        )

        self.df_ok = df_destino_ok

        return self.df_ok, self.df_erro

    def salvar_resultados(self, caminho_base="resultados_desdobramento", formato="xlsx"):
        """
        Salva df_ok e df_erro em arquivos.

        Parameters:
            caminho_base (str): Nome do diretório ou prefixo do arquivo.
                                Se for um diretório, os arquivos serão salvos dentro dele.
            formato (str): Formato de arquivo ('csv' ou 'xlsx'). Default é 'csv'.
        """
        if self.df_ok is None:
            print("Erro: O método desdobrar() deve ser executado antes de salvar.")
            return

        if formato.lower() not in ["csv", "xlsx"]:
            print(f"Formato '{formato}' não suportado. Usando 'csv'.")
            formato = "csv"
        
        # Cria o diretório se o caminho_base for um diretório
        if not caminho_base.endswith(f".{formato}"):
             os.makedirs(caminho_base, exist_ok=True)
             caminho_ok = os.path.join(caminho_base, f"df_desdobrado.{formato}")
             caminho_erro = os.path.join(caminho_base, f"df_erro.{formato}")
        else:
             # Se for um nome de arquivo completo, usa-o como prefixo
             caminho_ok = caminho_base.replace(f".{formato}", f"_desdobrado.{formato}")
             caminho_erro = caminho_base.replace(f".{formato}", f"_erros.{formato}")

        print(f"\nSalvando resultados no formato '{formato.upper()}'...")

        # Salva df_ok
        if formato == "csv":
            self.df_ok.to_csv(caminho_ok, index=False)
            self.df_erro.to_csv(caminho_erro, index=False)
        elif formato == "xlsx":
            with pd.ExcelWriter(caminho_ok, engine='xlsxwriter') as writer:
                self.df_ok.to_excel(writer, sheet_name='Desdobrado', index=False)
            with pd.ExcelWriter(caminho_erro, engine='xlsxwriter') as writer:
                self.df_erro.to_excel(writer, sheet_name='Erros', index=False)

        print(f"Salvo OK: {caminho_ok}")
        print(f"Salvo ERRO: {caminho_erro}")