import pandas as pd
import numpy as np
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

        # Identificar origem sem destino correspondente
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
        print(f"\n**Relatório de Desdobramento:**")
        print(f"Soma total de Origem: {self.soma_origem_total:,.2f}")
        print(f"Valor não distribuído (Erros): {soma_erro:,.2f}")

        # se nada sobrou, retorna vazio
        if df_origem_valida.empty:
            self.df_ok = pd.DataFrame()
            return self.df_ok, self.df_erro

        # Filtrar destino pelas chaves válidas
        df_destino_ok = df_destino.merge(
            df_origem_valida[chaves_comuns].drop_duplicates(),
            on=chaves_comuns,
            how="inner"
        )

        # Agrupar e criar pesos
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

class DataFrameDiagnostics:
    """
    Classe utilitária para diagnosticar problemas em DataFrames,
    especialmente focada em problemas de merge e tipagem.
    """
    def __init__(self):
        pass

    def prints_uteis(self, df: pd.DataFrame, nome: str ="DataFrame"):
        """
        Exibe uma visão geral técnica do DataFrame: Tipos, Nulos e Estatísticas.

        Args:
            df (pd.DataFrame):
                Dataframe a ser diagnosticado.
            nome (str):
                Nome para diferenciar nos prints, caso esteja vendo mais de um
                DataFrame.
        """
        print(f"\n{'=' * 60}")
        print(f"🔎 PRINTS UTEIS: {nome}")
        print(f"{'=' * 60}")

        print(f"Formato (Linhas, Colunas): {df.shape}")

        # Tipagem e Nulos
        print("\n--- 1. Tipagem e Nulos (Amostra) ---")
        info_df = pd.DataFrame({
            'Dtype': df.dtypes,
            'Nulos': df.isnull().sum(),
            '% Nulos': (df.isnull().sum() / len(df)) * 100,
            'Exemplo Unico': [
                df[c].dropna().unique()[0] if not df[c].dropna().empty else np.nan
                for c in df.columns
            ]
        })
        print(info_df)

        # Estatísticas básicas (apenas numéricas)
        print("\n--- 2. Describe (Numérico) ---")
        try:
            print(df.describe())
        except Exception:
            print("Não há colunas numéricas para descrever.")

        print(f"\n{'=' * 60}\n")

    def diagnosticar_merge(
        self,
        df_esq: pd.DataFrame,
        df_dir: pd.DataFrame,
        chave_esq: list,
        chave_dir: list | None = None,
        nome_esq: str = "Esq",
        nome_dir: str = "Dir"
    ):
        """
        Analisa possíveis razões de falha em um merge entre dois DataFrames.
        Verifica: Tipos, espaços em branco, case e interseção de chaves.
        Mostra: Todos os dados que não batem nas colunas

        Args:
            df_esq (pd.DataFrame):
                DataFrame a "esquerda" no merge
            df_dir (pd.DataFrame):
                DataFrame a "direita" no merge
            chave_esq (list):
                Lista com colunas que serão a chave no merge, caso
                as colunas tenha nomes diferentes essa lista se refere
                ao DataFrame a esquerda.
            chave_dir (list):
                Lista com colunas que serão a chave no merge, caso
                as colunas tenha nomes diferentes essa lista se refere
                ao DataFrame a direita.
            nome_esq (str):
                Nome para identificação nos prints
            nome_dir (str):
                Nome para identificação nos prints
        """
        if chave_dir is None:
            chave_dir = chave_esq

        print(f"\n{'=' * 60}")
        print(f"DIAGNÓSTICO DE MERGE: {nome_esq} vs {nome_dir}")
        print(f"Chaves: '{chave_esq}' (Esq) vs '{chave_dir}' (Dir)")
        print(f"{'=' * 60}")

        # Checagem de Tipagem
        # Converter para lista se for string
        chaves_esq = [chave_esq] if isinstance(chave_esq, str) else chave_esq
        chaves_dir = [chave_dir] if isinstance(chave_dir, str) else chave_dir

        print("\n1. Comparação de Tipos:")
        tipos_match = True
        for col_esq, col_dir in zip(chaves_esq, chaves_dir):
            type_esq = df_esq[col_esq].dtype
            type_dir = df_dir[col_dir].dtype
            match_str = "✅" if type_esq == type_dir else "⚠️"
            print(f"   {match_str} {col_esq:20} ({type_esq}) vs {col_dir:20} ({type_dir})")
            if type_esq != type_dir:
                tipos_match = False
                if 'int' in str(type_esq) and 'obj' in str(type_dir):
                    print(f"      -> Dica: '{col_dir}' é texto e '{col_esq}' é inteiro.")

        if tipos_match:
            print("   ✅ Todos os tipos coincidem.")
        else:
            print("   ⚠️ ALERTA: Há diferenças de tipo. O merge pode falhar.")

        # Análise de Conteúdo (Amostra)
        # Para múltiplas colunas, vamos fazer análise por coluna
        print("\n2. Análise de Valores Únicos:")
        total_match = 0
        
        for col_esq, col_dir in zip(chaves_esq, chaves_dir):
            set_esq = set(df_esq[col_esq].dropna().unique())
            set_dir = set(df_dir[col_dir].dropna().unique())
            interseccao = set_esq.intersection(set_dir)
            
            qtd_esq = len(set_esq)
            qtd_dir = len(set_dir)
            qtd_match = len(interseccao)
            total_match += qtd_match
            
            print(f"\n   Coluna '{col_esq}' vs '{col_dir}':")
            print(f"    - Únicos em {nome_esq}: {qtd_esq}")
            print(f"    - Únicos em {nome_dir}: {qtd_dir}")
            print(f"    - 🔗 Chaves em Comum: {qtd_match}")
            
            if qtd_match == 0:
                print(f"    ❌ CRÍTICO: Nenhuma chave corresponde em '{col_esq}'!")
            elif qtd_match < min(qtd_esq, qtd_dir) * 0.1:
                print(f"    ⚠️ ALERTA: Menos de 10% das chaves correspondem em '{col_esq}'.")

        # Detetive de Espaços (Whitespace)
        print("\n3. Investigação de Strings (Possível erro de espaço):")
        for col_esq, col_dir in zip(chaves_esq, chaves_dir):
            if df_esq[col_esq].dtype == 'O' or df_dir[col_dir].dtype == 'O':
                sample_esq = str(df_esq[col_esq].dropna().iloc[0]) if len(df_esq[col_esq].dropna()) > 0 else ""
                sample_dir = str(df_dir[col_dir].dropna().iloc[0]) if len(df_dir[col_dir].dropna()) > 0 else ""
                
                has_space = (len(sample_esq.strip()) != len(sample_esq) or 
                            len(sample_dir.strip()) != len(sample_dir))
                
                if has_space:
                    print(f"   ⚠️ '{col_esq}': '{sample_esq}' (len={len(sample_esq)}) - Detectado espaço!")
                    print(f"   ⚠️ '{col_dir}': '{sample_dir}' (len={len(sample_dir)}) - Detectado espaço!")
                else:
                    print(f"   ✅ '{col_esq}' e '{col_dir}': Sem espaços detectados.")

        # Identificar os Vilões (valores que não fazem match)
        print("\n4. 🔍 Valores que NÃO vão fazer match (Os Vilões):")
        for col_esq, col_dir in zip(chaves_esq, chaves_dir):
            set_esq = set(df_esq[col_esq].dropna().unique())
            set_dir = set(df_dir[col_dir].dropna().unique())
            
            viloes_esq = set_esq - set_dir  # Estão em esq mas não em dir
            viloes_dir = set_dir - set_esq  # Estão em dir mas não em esq
            
            print(f"\n   📍 Coluna '{col_esq}':")
            
            if viloes_esq:
                print(f"      ❌ Em {nome_esq} mas NÃO em {nome_dir} ({len(viloes_esq)} valores):")
                # Mostra todos os casos
                for valor in list(viloes_esq)[:]: # <- Caso queira que apareça menos inserir valor a direita 
                    qtd_ocorr = len(df_esq[df_esq[col_esq] == valor])
                    print(f"         - {valor} ({qtd_ocorr}x)")
            else:
                print(f"      ✅ Todos os valores de {nome_esq} existem em {nome_dir}")
            
            if viloes_dir:
                print(f"      ❌ Em {nome_dir} mas NÃO em {nome_esq} ({len(viloes_dir)} valores):")
                # Mostra todos os casos
                for valor in list(viloes_dir)[:]: # <- Caso queira que apareça menos inserir valor a direita
                    qtd_ocorr = len(df_dir[df_dir[col_dir] == valor])
                    print(f"         - {valor} ({qtd_ocorr}x)")
            else:
                print(f"      ✅ Todos os valores de {nome_dir} existem em {nome_esq}")

        print(f"{'=' * 60}\n")