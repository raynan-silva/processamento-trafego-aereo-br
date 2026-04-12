"""
Pipeline PySpark para processamento do dataset de tráfego aéreo brasileiro (ANAC 2000-2025).

Este script realiza o download, amostragem, limpeza, análise exploratória e
persistência em Parquet de um CSV massivo (~20 GB) em um ambiente com apenas
20 GB de RAM, utilizando Apache Spark como motor de processamento.
"""

import glob
import logging
import os
import sys

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

def perform_eda(df: DataFrame) -> None:
    """Executa análises exploratórias e exibe os resultados no console.

    Análises realizadas:

    * **Passageiros pagos por companhia aérea:** agregação por ``sg_empresa_icao``.
    * **Top 5 rotas mais movimentadas:** agrupamento por par
      (``sg_icao_origem``, ``sg_icao_destino``) ordenado por total de passageiros.

    Args:
        df: DataFrame Spark limpo e transformado.
    """
    # --- Passageiros pagos por companhia ---
    logger.info("=== Total de passageiros pagos por companhia aérea ===")
    passengers_by_airline: DataFrame = (
        df.groupBy("sg_empresa_icao")
        .agg(F.sum("nr_passag_pagos").alias("total_passag_pagos"))
        .orderBy(F.col("total_passag_pagos").desc())
    )
    passengers_by_airline.show(truncate=False)

    # --- Top 5 rotas mais movimentadas ---
    logger.info("=== Top 5 rotas mais movimentadas ===")
    top_routes: DataFrame = (
        df.groupBy("sg_icao_origem", "sg_icao_destino")
        .agg(F.sum("nr_passag_pagos").alias("total_passag_pagos"))
        .orderBy(F.col("total_passag_pagos").desc())
        .limit(5)
    )
    top_routes.show(truncate=False)


# ---------------------------------------------------------------------------
# 6. Salvamento em Parquet
# ---------------------------------------------------------------------------

def save_to_parquet(df: DataFrame, output_path: str) -> None:
    """Salva o DataFrame processado em formato Parquet particionado.

    O formato Parquet é colunar e comprimido, sendo ideal para análises
    futuras com leitura parcial de colunas e predicados push-down.

    Args:
        df: DataFrame Spark a ser persistido.
        output_path: Caminho do diretório de destino para os arquivos Parquet.

    Raises:
        RuntimeError: Se a escrita falhar.
    """
    try:
        df.write.mode("overwrite").parquet(output_path)
        logger.info("Dados salvos em Parquet: %s", output_path)
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
        perform_eda(df_clean)

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
