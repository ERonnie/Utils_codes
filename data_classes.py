import pandas as pd
import numpy as np
import time
import os

class MegaDesdobrador:
    def __init__(self):
        self.df_ok = None
        self.df_erro = None
        self.soma_origem_total = 0

    def desdobrar_classico(self, df_origem, df_destino, chaves_origem, chaves_destino, coluna_valor):
        """
        Desdobra valores da origem baseando-se no peso atual do destino.
        Ideal para: Abrir Demanda em Itens/Cidades que já possuem valores no destino.
        """
        inicio_proc = time.time()
        self.soma_origem_total = df_origem[coluna_valor].sum()
        chaves_comuns = [c for c in chaves_origem if c in chaves_destino]

        # Identificar erros (Origem sem par no Destino)
        check = df_origem.merge(df_destino[chaves_comuns].drop_duplicates(), on=chaves_comuns, how="left", indicator=True)
        self.df_erro = check[check["_merge"] == "left_only"].drop(columns="_merge")
        df_origem_valida = check[check["_merge"] == "both"].drop(columns="_merge")

        # Calcular Pesos no Destino
        destino_agrupado = df_destino.groupby(chaves_comuns)[coluna_valor].sum().reset_index().rename(columns={coluna_valor: "soma_destino"})
        df_destino_ok = df_destino.merge(destino_agrupado, on=chaves_comuns, how="left")
        
        df_destino_ok["peso"] = np.where(df_destino_ok["soma_destino"] != 0, 
                                         df_destino_ok[coluna_valor] / df_destino_ok["soma_destino"], 0)

        # Aplicar Desdobramento
        df_destino_ok = df_destino_ok.merge(df_origem_valida[chaves_origem + [coluna_valor]], 
                                            on=chaves_comuns, how="left", suffixes=("", "_origem"))
        
        df_destino_ok["valor_desdobrado"] = df_destino_ok["peso"] * df_destino_ok[f"{coluna_valor}_origem"]
        
        self.df_ok = df_destino_ok.drop(columns=["soma_destino", "peso", f"{coluna_valor}_origem"])
        
        self._exibir_auditoria("CLÁSSICO", self.soma_origem_total, self.df_ok["valor_desdobrado"].sum(), self.df_erro[coluna_valor].sum(), time.time() - inicio_proc)
        return self.df_ok, self.df_erro

    def desdobrar_complexo(self, df_demanda, df_historico, df_lote, chaves_ligacao, chaves_detalhamento, coluna_valor, pivotar=True):
        """
        Projeta a demanda baseando-se no histórico de 6 meses e aplica lote mínimo.
        Ideal para: Abrir Forecast consolidado em granularidade SKU/UF/Cidade.
        """
        inicio_proc = time.time()
        self.soma_origem_total = df_demanda[coluna_valor].sum()
        chaves_full = chaves_ligacao + chaves_detalhamento
        
        # Share Histórico
        dist_hist = df_historico.groupby(chaves_full)[coluna_valor].sum().reset_index()
        dist_hist[coluna_valor] = np.where(dist_hist[coluna_valor] < 0, 0.5, dist_hist[coluna_valor])
        soma_grupo = dist_hist.groupby(chaves_ligacao)[coluna_valor].transform('sum')
        dist_hist['fator'] = dist_hist[coluna_valor] / soma_grupo.replace(0, 1)

        # Projeção
        df_merged = df_demanda.merge(dist_hist[chaves_full + ['fator']], on=chaves_ligacao, how='left')
        df_merged['valor_desdobrado'] = df_merged[coluna_valor] * df_merged['fator']

        # Lote Mínimo
        df_merged = df_merged.merge(df_lote[['Item', 'Lote_Multiplo']], on='Item', how='left').fillna({'Lote_Multiplo': 0})
        # Regra Lote: < 0.5 vira 0 | entre 0.5 e 1.0 vira Lote
        df_merged['valor_final'] = np.where(df_merged['valor_desdobrado'] < (0.5 * df_merged['Lote_Multiplo']), 0, df_merged['valor_desdobrado'])
        cond_lote = (df_merged['valor_desdobrado'] >= (0.5 * df_merged['Lote_Multiplo'])) & (df_merged['valor_desdobrado'] <= df_merged['Lote_Multiplo'])
        df_merged['valor_final'] = np.where(cond_lote, df_merged['Lote_Multiplo'], df_merged['valor_final'])

        # Separação
        self.df_ok = df_merged[df_merged['fator'].notna() & (df_merged['valor_final'] > 0)].copy()
        self.df_erro = df_demanda[~df_demanda.set_index(chaves_ligacao).index.isin(dist_hist.set_index(chaves_ligacao).index)].copy()

        self._exibir_auditoria("COMPLEXO", self.soma_origem_total, self.df_ok['valor_final'].sum(), self.df_erro[coluna_valor].sum(), time.time() - inicio_proc)

        if pivotar and not self.df_ok.empty:
            return self._executar_pivot(chaves_full, 'valor_final'), self.df_erro
        return self.df_ok, self.df_erro

    # AUXILIARES
    def _executar_pivot(self, chaves_index, col_valor):
        return self.df_ok.pivot_table(index=chaves_index, columns='AnoMes', values=col_valor, aggfunc='sum').reset_index()

    def _exibir_auditoria(self, modo, v_in, v_out, v_err, tempo):
        taxa_erro = (v_err / v_in) * 100 if v_in > 0 else 0
        print(f"\n>>> RELATÓRIO MEGA DESDOBRADOR | MODO: {modo}")
        print(f"{'-'*50}")
        print(f"Soma Origem:     {v_in:>15.2f}")
        print(f"Soma Desdobrado: {v_out:>15.2f}")
        print(f"Soma Erros:      {v_err:>15.2f} ({taxa_erro:.1f}%)")
        print(f"Status:          {'✅ DENTRO DA MARGEM' if taxa_erro <= 5 else '⚠️ FORA DA MARGEM'}")
        print(f"Tempo:           {tempo:.2f}s")
        print(f"{'-'*50}")

    def salvar_resultados(self, caminho_base="outputs", formato="xlsx"):
        # Implementação de salvamento robusta (semelhante à que você enviou)
        os.makedirs(caminho_base, exist_ok=True)
        if formato == "xlsx":
            self.df_ok.to_excel(f"{caminho_base}/resultado_ok.xlsx", index=False)
            self.df_erro.to_excel(f"{caminho_base}/resultado_erros.xlsx", index=False)
        else:
            self.df_ok.to_csv(f"{caminho_base}/resultado_ok.csv", index=False)
            self.df_erro.to_csv(f"{caminho_base}/resultado_erros.csv", index=False)

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