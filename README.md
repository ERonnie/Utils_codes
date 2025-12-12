# 📊 Data Utils - Ferramentas de Manipulação de Dados

Este repositório contém um conjunto de funções utilitárias em Python projetadas para **simplificar e padronizar** tarefas comuns de manipulação de dados, como **carregar, salvar e pré-processar** arquivos (CSV, Excel).

As funções utilizam principalmente as bibliotecas **Pandas** e **Polars**, permitindo flexibilidade e performance otimizada, especialmente no carregamento de arquivos grandes.

---

## 🚀 Instalação

Para utilizar este módulo, certifique-se de ter o Python instalado. As seguintes bibliotecas são necessárias:

```bash
pip install pandas polars openpyxl pyxlsb
```

## 🛠️ Funções Disponíveis

1. ``carregar_arquivo``\
Carrega arquivos de dados, suportando múltiplos formatos e permitindo a escolha entre Pandas e Polars como engine de leitura. Já inclui lógicas de tratamento de erros de codificação (utf-8 e latin1) e pré-processamento básico (limpeza de espaços e conversão para maiúsculas).

### Parâmetros

|**Parâmetro** | **Tipo** | **Descrição** | **Padrão**|
|--------------|----------|---------------|-----------|
|``caminho``|``str``| Local do arquivo a ser carregado| Obrigatório
|``engine``|``str``| Biblioteca a ser usada: 'pandas' ou 'polars'. Polars é recomendado para arquivos grandes.| ``'pandas'``
|``limpar``|``bool``| Se ``True``, remove espaços em branco nas extremidades dos dados em colunas do tipo ``object`` (string).| ``False``
|``uppercase``|``bool``| Se ``True``, transforma os dados em colunas do tipo ``object`` em **caixa alta.**| ``False``
|``**kwargs``|``dict``| Argumentos adicionais de leitura (ex: header, sheet_name) que são passados para a função de leitura, respeitando a sintaxe da engine escolhida.| ``{}``

### Exemplo de Uso

```Python
from Utils_codes import carregar_arquivo

# Carregar CSV com Pandas (Padrão), limpando e convertendo para maiúsculas
df_pandas = carregar_arquivo("dados.csv", limpar=True, uppercase=True)

# Carregar XLSX com Polars (para performance)
df_polars = carregar_arquivo("dados_grandes.xlsx", engine="polars")
```

2. ``salvar_arquivo``\
Salva um DataFrame (Pandas) em um arquivo, com padrões definidos para formatação e nomenclatura, garantindo consistência nas saídas.

### Parâmetros

|**Parâmetro** | **Tipo** | **Descrição** | **Padrão**|
|--------------|----------|---------------|-----------|
|``df``| ``pd.DataFrame``| DataFrame que será salvo. | Obrigatório
|``nome_arquivo``| ``str``| **Nome de saída** do arquivo (sem extensão). | Obrigatório
|``caminho``| ``str``| **Local de saída do arquivo.** Se ``None``, usa o diretório atual. | ``os.getcwd()``
|``extensao``| ``str``| Extensão desejada: ``'csv'`` ou ``'excel'`` (.xlsx). | ``'csv'``
|``**kwargs``| ``dict``| Argumentos adicionais padrão Pandas (ex: ``encoding``, ``sheet_name``). | ``{}``

#### Padrões de Saída:

- **CSV**: ``sep=";"``, ``decimal=","``, ``encoding="utf-8"``, ``index=False``
- **Excel**: ``sheet_name="BD_Python"``, ``index=False``

### Exemplo de Uso

```Python
from Utils_codes import salvar_arquivo
import pandas as pd

df = pd.DataFrame({'Col1': [1, 2], 'Col2': ['A', 'B']})

# Salvar como CSV no diretório atual
salvar_arquivo(df, "minha_saida")
```

3. ``ajustar_data``\
Ajusta uma coluna de um DataFrame para o formato de data padronizado yyyy-mm-dd. Inclui lógica para tratar strings no formato 'YYYY-MM' ou 'YYYY/MM' adicionando o dia '01' e reporta valores inválidos.

### Parâmetros

|**Parâmetro** | **Tipo** | **Descrição** | **Padrão**|
|--------------|----------|---------------|-----------|
|``df``| ``pd.DataFrame``| DataFrame a ser ajustado. | Obrigatório
|``coluna``| ``str``| Nome da coluna de data a ser padronizada. | Obrigatório
|``reportar_erros``| ``bool``| Se ``True``, imprime uma lista dos valores que não puderam ser convertidos para data (``NaT``). | ``True``

### Exemplo de Uso

```Python
from Utils_codes import ajustar_data

df_ajustado = ajustar_data(df, "Data_Venda")
```

4. ``ajustar_colunas``\
Ajusta o case (caixa alta ou baixa) dos dados em todas as colunas do tipo object (string) do DataFrame.

### Parâmetros

|**Parâmetro** | **Tipo** | **Descrição** | **Padrão**|
|--------------|----------|---------------|-----------|
|``df``| ``pd.DataFrame``| DataFrame a ser ajustado. | Obrigatório
|``ajustar_para``| ``str``| Opções: ``'maisculas'`` (converte para UPPERCASE) ou ``'minusculas'`` (converte para lowercase).| ``maisculas``

### Exemplo de Uso

```Python
from Utils_codes import ajustar_colunas

# Converte todas as strings do DF para MAIÚSCULAS
df_upper = ajustar_colunas(df, ajustar_para="maisculas")
```

## 🛠️ Classes Disponiveis

1. ``DesdobradorProporcional``\
Classe aumenta o nivel de detalhe de um DataFrame para o mesmo nivel de outro DataFrame

### Exemplo de Uso

```Python
from Utils_codes import DesdobradorProporcional as DesdProp

# Determinando contantes
DETALHE_MENOR = ['UF', 'SKU', 'COD_FILIAL']
DETALHE_MAIOR = ['UF', 'SKU', 'COD_FILIAL',
                 'ORIGEM', 'FATURAMENTO', 'DESTINO']
COLUNA_DESDOBRAR = "Volume_orig"

# Instacia recebe valores a serem abertos
desdobrador = DesdProp(
    df_detalhe_menor,
    df_detalhe_maior,
    DETALHE_MENOR,
    DETALHE_MAIOR,
    COLUNA_DESDOBRAR
    )

# Metodo que executa o aumento de detalhe
desdobrador.desdobrar()
# Metodo salva em uma pasta ou diretorio
desdobrador.salvar_resultados("Arquivos_finais", "xlsx")
```