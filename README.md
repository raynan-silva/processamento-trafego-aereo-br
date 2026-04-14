# PySpark ANAC Air Traffic Pipeline

Pipeline de dados em **Apache Spark (PySpark)** para processamento de um dataset massivo (~20 GB) de tráfego aéreo brasileiro publicado pela **ANAC** (Agência Nacional de Aviação Civil), cobrindo o período de 2000 a 2025.

O projeto demonstra como processar um arquivo CSV maior que a memória RAM disponível (20 GB de RAM) usando PySpark, com amostragem, limpeza, análise exploratória e persistência em Parquet.

---

## Visão Geral

| Etapa | Função | Descrição |
|---|---|---|
| 1 | `download_dataset()` | Baixa o dataset do Kaggle via `kagglehub` |
| 2 | `create_spark_session()` | Cria a SparkSession com memória limitada (8–12g) |
| 3 | `load_and_sample_data()` | Lê o CSV e aplica amostragem de 2% |
| 4 | `clean_and_transform()` | Filtra voos regulares/extras e trata nulos |
| 5 | `perform_eda()` | 7 análises exploratórias (companhias, rotas, evolução anual, sazonalidade, doméstico/internacional, aeroportos, market share) |
| 6 | `save_to_parquet()` | Salva resultado limpo em formato Parquet |

---

## Pré-requisitos

| Dependência | Versão mínima |
|---|---|
| Python | 3.10+ |
| Java (JDK) | 17+ |
| PySpark | 4.0+ |
| kagglehub | 1.0+ |
| matplotlib | 3.8+ |
| pandas | 2.0+ |

Além disso, é necessário ter uma conta no [Kaggle](https://www.kaggle.com/) com o token de API configurado (veja a seção [Configurar Kaggle API](#configurar-kaggle-api)).

---

## Instalação e Execução

### Linux (Ubuntu/Debian)

#### 1. Instalar o JDK 17

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk
```

Verifique a instalação:

```bash
java -version
```

#### 2. Clonar o repositório

```bash
git clone https://github.com/seu-usuario/pyspark-anac-air-traffic-pipeline.git
cd pyspark-anac-air-traffic-pipeline
```

#### 3. Criar ambiente virtual e instalar dependências

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install pyspark kagglehub
```

#### 4. Configurar Kaggle API

Acesse [kaggle.com/settings](https://www.kaggle.com/settings) → seção "API" → **Create New Token**. O arquivo `kaggle.json` será baixado. Mova-o para o diretório correto:

```bash
mkdir -p ~/.kaggle
mv ~/Downloads/kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json
```

#### 5. Executar o pipeline

```bash
python pipeline_anac.py
```

---

### Windows

#### 1. Instalar o JDK 17

Baixe e instale o [Eclipse Temurin JDK 17](https://adoptium.net/temurin/releases/?version=17) (marque a opção "Set JAVA_HOME" durante a instalação).

Verifique no PowerShell:

```powershell
java -version
```

#### 2. Clonar o repositório

```powershell
git clone https://github.com/seu-usuario/pyspark-anac-air-traffic-pipeline.git
cd pyspark-anac-air-traffic-pipeline
```

#### 3. Criar ambiente virtual e instalar dependências

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install pyspark kagglehub
```

> **Nota:** Se ocorrer erro de execução de scripts no PowerShell, execute antes:
> ```powershell
> Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
> ```

#### 4. Configurar Kaggle API

Baixe o `kaggle.json` em [kaggle.com/settings](https://www.kaggle.com/settings) → "API" → **Create New Token**. Coloque-o em:

```
C:\Users\<SeuUsuario>\.kaggle\kaggle.json
```

#### 5. Configurar variável `HADOOP_HOME` (Windows apenas)

O PySpark no Windows requer o `winutils.exe`. Baixe-o de [github.com/cdarlint/winutils](https://github.com/cdarlint/winutils) (escolha a versão compatível com Hadoop 3.x) e configure:

```powershell
# Exemplo: salve winutils.exe em C:\hadoop\bin\
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
```

Reinicie o terminal após definir a variável.

#### 6. Executar o pipeline

```powershell
python pipeline_anac.py
```

---

## Saída Esperada

O pipeline imprime no console:

1. **Top 10 companhias por passageiros pagos**
2. **Top 10 rotas mais movimentadas** (pares origem → destino)
3. **Evolução anual do tráfego aéreo** (passageiros + voos por ano)
4. **Sazonalidade mensal** (passageiros por mês)
5. **Distribuição doméstico vs. internacional**
6. **Top 10 aeroportos mais movimentados** (origem + destino)
7. **Market share das companhias aéreas** (%)

Os dados limpos são salvos em `output_parquet/` no formato Apache Parquet.

### Visualizações (Notebook)

Abra o notebook `analise_visual.ipynb` — ele é **autossuficiente** e executa todo o pipeline automaticamente:

```bash
jupyter notebook analise_visual.ipynb
```

Na primeira execução, o notebook baixa o dataset, processa e salva em Parquet.
Nas execuções seguintes, ele detecta o Parquet existente e carrega direto (muito mais rápido).

O notebook inclui 10 visualizações, incluindo análise do impacto da COVID-19 e resumo estatístico.

> **Nota:** O `pipeline_anac.py` também pode ser executado de forma independente via terminal (`python pipeline_anac.py`) — ele imprime as análises no console e gera o Parquet.

---

## Estrutura do Projeto

```
.
├── pipeline_anac.py      # Script principal do pipeline ETL
├── analise_visual.ipynb  # Notebook com visualizações gráficas
├── pyproject.toml        # Metadados e dependências do projeto
├── README.md             # Este arquivo
├── .gitignore
└── output_parquet/       # Gerado após execução (não versionado)
```

---

## Sobre os Dados

- **Fonte:** [Brazil Air Traffic Data 2000-2025 (ANAC)](https://www.kaggle.com/datasets/sturarods/anac-national-civil-aviation-agency-2000-2025) no Kaggle
- **Tamanho:** ~20,15 GB (CSV)
- **Registros:** ~22 milhões de etapas de voo
- **Colunas:** 111 variáveis operacionais

Variáveis-chave utilizadas no pipeline:

| Coluna | Descrição |
|---|---|
| `sg_empresa_icao` | Sigla ICAO da companhia aérea |
| `cd_di` | Dígito Identificador (0 = Regular, 2 = Extra) |
| `sg_icao_origem` | Aeródromo de origem (ICAO) |
| `sg_icao_destino` | Aeródromo de destino (ICAO) |
| `nr_passag_pagos` | Passageiros pagos transportados |
| `kg_carga_paga` | Carga paga em kg |
| `nr_ano_referencia` | Ano de referência |
| `nr_mes_referencia` | Mês de referência |
| `ds_natureza_etapa` | Natureza da etapa (Doméstica/Internacional) |

---

## Licença

MIT
