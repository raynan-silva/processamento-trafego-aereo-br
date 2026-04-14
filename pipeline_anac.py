"""
Pipeline PySpark para processamento do dataset de tráfego aéreo brasileiro (ANAC 2000-2025).

Este script realiza o download, amostragem, limpeza, análise exploratória e
persistência em Parquet de um CSV massivo (~20 GB) em um ambiente com apenas
20 GB de RAM, utilizando Apache Spark como motor de processamento.
"""

import glob
import logging
import os

import kagglehub
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Download do dataset
# ---------------------------------------------------------------------------

def download_dataset() -> str:
    """Baixa o dataset da ANAC via ``kagglehub`` e retorna o caminho local.

    Returns:
        str: Caminho absoluto do diretório contendo os arquivos baixados.

    Raises:
        RuntimeError: Se o download falhar por qualquer motivo.
    """
    try:
        path: str = kagglehub.dataset_download(
            "sturarods/anac-national-civil-aviation-agency-2000-2025"
        )
        logger.info("Dataset baixado em: %s", path)
        return path
    except Exception as exc:
        raise RuntimeError(f"Falha ao baixar o dataset: {exc}") from exc


# ---------------------------------------------------------------------------
# 2. Criação da SparkSession
# ---------------------------------------------------------------------------

def create_spark_session(driver_memory: str = "8g") -> SparkSession:
    """Cria e retorna uma SparkSession com configurações defensivas de memória.

    Args:
        driver_memory: Quantidade de memória alocada para o driver Spark.
            Deve ser conservador em relação à RAM total do sistema (padrão ``"8g"``).

    Returns:
        SparkSession: Sessão Spark configurada e pronta para uso.
    """
    spark: SparkSession = (
        SparkSession.builder
        .appName("ANAC-AirTraffic-Pipeline")
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        "SparkSession criada — driver_memory=%s", driver_memory
    )
    return spark


# ---------------------------------------------------------------------------
# 3. Leitura e amostragem
# ---------------------------------------------------------------------------

def load_and_sample_data(
    spark: SparkSession,
    file_path: str,
    fraction: float = 0.02,
    seed: int = 42,
) -> DataFrame:
    """Lê o(s) CSV(s) do dataset e aplica amostragem imediata.

    A amostragem é essencial para viabilizar o processamento em um ambiente
    com RAM limitada (20 GB) diante de um CSV de ~20 GB.

    Args:
        spark: SparkSession ativa.
        file_path: Caminho para o arquivo CSV ou diretório contendo CSVs.
        fraction: Fração da amostra (entre 0.0 e 1.0). Padrão ``0.02`` (2 %).
        seed: Semente para reprodutibilidade da amostragem.

    Returns:
        DataFrame: DataFrame Spark já amostrado.

    Raises:
        FileNotFoundError: Se nenhum arquivo CSV for encontrado no caminho.
        RuntimeError: Se a leitura do CSV falhar.
    """
    csv_files: list[str] = glob.glob(os.path.join(file_path, "**", "*.csv"), recursive=True)
    if not csv_files:
        raise FileNotFoundError(f"Nenhum arquivo CSV encontrado em: {file_path}")

    logger.info("Arquivos CSV encontrados: %s", csv_files)

    try:
        df: DataFrame = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("sep", ",")
            .option("encoding", "iso-8859-1")
            .csv(csv_files)
        )
    except Exception as exc:
        raise RuntimeError(f"Erro ao ler CSV(s): {exc}") from exc

    logger.info("Colunas detectadas (%d): %s", len(df.columns), df.columns[:10])

    sampled_df: DataFrame = df.sample(withReplacement=False, fraction=fraction, seed=seed)
    logger.info(
        "Amostragem aplicada (fraction=%.2f). Linhas estimadas na amostra: %d",
        fraction,
        sampled_df.count(),
    )
    return sampled_df


# ---------------------------------------------------------------------------
# 4. Limpeza e transformação
# ---------------------------------------------------------------------------

def clean_and_transform(df: DataFrame) -> DataFrame:
    """Filtra e limpa o DataFrame amostrado.

    Etapas realizadas:

    1. Filtra apenas registros de **Etapa Regular** (``0``) e **Etapa Extra** (``2``)
       com base na coluna ``cd_di``.
    2. Preenche valores nulos em ``nr_passag_pagos`` e ``kg_carga_paga`` com ``0``.

    Args:
        df: DataFrame Spark bruto (já amostrado).

    Returns:
        DataFrame: DataFrame limpo e pronto para análise.
    """
    df_filtered: DataFrame = df.filter(
        "CAST(cd_di AS STRING) IN ('0', '2')"
    )
    logger.info(
        "Registros após filtro cd_di ∈ {0, 2}: %d",
        df_filtered.count(),
    )

    numeric_fill_cols: list[str] = ["nr_passag_pagos", "kg_carga_paga"]
    existing_cols: list[str] = df_filtered.columns
    fill_map: dict[str, int] = {
        c: 0 for c in numeric_fill_cols if c in existing_cols
    }
    df_clean: DataFrame = df_filtered.fillna(fill_map)

    logger.info("Valores nulos tratados em: %s", list(fill_map.keys()))
    return df_clean


# ---------------------------------------------------------------------------
# 5. Análise exploratória (EDA)
# ---------------------------------------------------------------------------

def perform_eda(df: DataFrame) -> dict[str, DataFrame]:
    """Executa análises exploratórias e exibe os resultados no console.

    Análises realizadas:

    1. **Passageiros pagos por companhia aérea** (top 10).
    2. **Top 10 rotas mais movimentadas** por passageiros.
    3. **Evolução anual do tráfego aéreo** (passageiros por ano).
    4. **Sazonalidade mensal** (passageiros médios por mês).
    5. **Distribuição doméstico vs. internacional**.
    6. **Top 10 aeroportos mais movimentados** (origem + destino).
    7. **Market share das companhias** (% de passageiros pagos).

    Args:
        df: DataFrame Spark limpo e transformado.

    Returns:
        dict[str, DataFrame]: Dicionário com os DataFrames resultantes de
        cada análise, para uso posterior (ex.: visualizações no notebook).
    """
    results: dict[str, DataFrame] = {}

    # --- 1. Top 10 companhias por passageiros pagos ---
    logger.info("=== Top 10 companhias por passageiros pagos ===")
    passengers_by_airline: DataFrame = (
        df.groupBy("sg_empresa_icao")
        .agg(F.sum("nr_passag_pagos").alias("total_passag_pagos"))
        .orderBy(F.col("total_passag_pagos").desc())
    )
    passengers_by_airline.show(10, truncate=False)
    results["passengers_by_airline"] = passengers_by_airline

    # --- 2. Top 10 rotas mais movimentadas ---
    logger.info("=== Top 10 rotas mais movimentadas ===")
    top_routes: DataFrame = (
        df.groupBy("sg_icao_origem", "sg_icao_destino")
        .agg(F.sum("nr_passag_pagos").alias("total_passag_pagos"))
        .orderBy(F.col("total_passag_pagos").desc())
        .limit(10)
    )
    top_routes.show(truncate=False)
    results["top_routes"] = top_routes

    # --- 3. Evolução anual do tráfego aéreo ---
    logger.info("=== Evolução anual do tráfego aéreo ===")
    yearly_traffic: DataFrame = (
        df.groupBy("nr_ano_referencia")
        .agg(
            F.sum("nr_passag_pagos").alias("total_passag_pagos"),
            F.count("*").alias("total_voos"),
        )
        .orderBy("nr_ano_referencia")
    )
    yearly_traffic.show(30, truncate=False)
    results["yearly_traffic"] = yearly_traffic

    # --- 4. Sazonalidade mensal (média de passageiros por mês) ---
    logger.info("=== Sazonalidade mensal (passageiros por mês) ===")
    monthly_seasonality: DataFrame = (
        df.groupBy("nr_mes_referencia")
        .agg(F.sum("nr_passag_pagos").alias("total_passag_pagos"))
        .orderBy("nr_mes_referencia")
    )
    monthly_seasonality.show(12, truncate=False)
    results["monthly_seasonality"] = monthly_seasonality

    # --- 5. Distribuição doméstico vs. internacional ---
    logger.info("=== Distribuição doméstico vs. internacional ===")
    nature_dist: DataFrame = (
        df.groupBy("ds_natureza_etapa")
        .agg(
            F.sum("nr_passag_pagos").alias("total_passag_pagos"),
            F.count("*").alias("total_voos"),
        )
        .orderBy(F.col("total_passag_pagos").desc())
    )
    nature_dist.show(truncate=False)
    results["nature_distribution"] = nature_dist

    # --- 6. Top 10 aeroportos mais movimentados (origem + destino) ---
    logger.info("=== Top 10 aeroportos mais movimentados ===")
    orig: DataFrame = (
        df.select(
            F.col("sg_icao_origem").alias("aeroporto"),
            F.col("nr_passag_pagos"),
        )
    )
    dest: DataFrame = (
        df.select(
            F.col("sg_icao_destino").alias("aeroporto"),
            F.col("nr_passag_pagos"),
        )
    )
    top_airports: DataFrame = (
        orig.union(dest)
        .groupBy("aeroporto")
        .agg(F.sum("nr_passag_pagos").alias("total_passag_pagos"))
        .orderBy(F.col("total_passag_pagos").desc())
        .limit(10)
    )
    top_airports.show(truncate=False)
    results["top_airports"] = top_airports

    # --- 7. Market share das companhias (%) ---
    logger.info("=== Market share das companhias aéreas (%) ===")
    total_passengers: int = (
        df.agg(F.sum("nr_passag_pagos")).collect()[0][0] or 0
    )
    market_share: DataFrame = (
        passengers_by_airline
        .withColumn(
            "market_share_pct",
            F.round(F.col("total_passag_pagos") / F.lit(total_passengers) * 100, 2),
        )
        .limit(10)
    )
    market_share.show(truncate=False)
    results["market_share"] = market_share

    return results


# ---------------------------------------------------------------------------
# 6. Salvamento em Parquet
# ---------------------------------------------------------------------------

def save_to_parquet(df: DataFrame, output_path: str, num_partitions: int = 8) -> None:
    """Salva o DataFrame processado em formato Parquet.

    O formato Parquet é colunar e comprimido, sendo ideal para análises
    futuras com leitura parcial de colunas e predicados push-down.

    Args:
        df: DataFrame Spark a ser persistido.
        output_path: Caminho do diretório de destino para os arquivos Parquet.
        num_partitions: Número de partições de saída (padrão ``8``).
            Utiliza ``coalesce`` para consolidar os arquivos gerados.

    Raises:
        RuntimeError: Se a escrita falhar.
    """
    try:
        df.coalesce(num_partitions).write.mode("overwrite").parquet(output_path)
        logger.info("Dados salvos em Parquet (%d partições): %s", num_partitions, output_path)
    except Exception as exc:
        raise RuntimeError(f"Erro ao salvar Parquet: {exc}") from exc


# ---------------------------------------------------------------------------
# Orquestrador
# ---------------------------------------------------------------------------

def main() -> None:
    """Função principal que orquestra todo o pipeline de processamento."""
    # 1. Download
    dataset_path: str = download_dataset()

    # 2. Spark
    spark: SparkSession = create_spark_session(driver_memory="12g")

    try:
        # 3. Leitura + amostragem (2 % do dataset)
        df_raw: DataFrame = load_and_sample_data(
            spark, dataset_path, fraction=0.02
        )

        # 4. Limpeza e transformação
        df_clean: DataFrame = clean_and_transform(df_raw)

        # 5. Análise exploratória
        eda_results: dict[str, DataFrame] = perform_eda(df_clean)
        logger.info("Análises concluídas: %s", list(eda_results.keys()))

        # 6. Persistência
        output_dir: str = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "output_parquet",
        )
        save_to_parquet(df_clean, output_dir)

    finally:
        spark.stop()
        logger.info("SparkSession encerrada.")


if __name__ == "__main__":
    main()
