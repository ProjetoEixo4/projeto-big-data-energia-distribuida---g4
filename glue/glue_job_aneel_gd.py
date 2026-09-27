"""
AWS Glue Job (PySpark) — ANEEL Geração Distribuída
Camada raw (S3, CSV) -> camada trusted (S3, Parquet particionado por ANO/UF)

Escolhido como job Glue (e não Lambda) por ser a fonte de maior volume do
projeto — o arquivo nacional consolidado excede o limite prático de memória
e tempo de execução do Lambda (15 min / poucos GB de RAM).

Parâmetros esperados (definidos no Glue Job como Job Parameters):
  --S3_RAW_PATH      s3://oet-datalake-raw/aneel_gd/
  --S3_TRUSTED_PATH  s3://oet-datalake-trusted/fato_geracao_distribuida/
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_RAW_PATH", "S3_TRUSTED_PATH"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

df = (
    spark.read.option("header", True)
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .csv(args["S3_RAW_PATH"])
)

# --- Tratamento de nulos: potência é a medida central, sem ela o registro é descartado ---
df = df.filter(F.col("MDA_POTENCIAINSTALADAKW").isNotNull())

# --- Município/distribuidora ausentes -> apontam para membro "desconhecido" da dimensão ---
df = df.withColumn(
    "COD_MUNICIPIO_IBGE",
    F.coalesce(F.col("COD_MUNICIPIO_IBGE"), F.lit("0000000")),
).withColumn(
    "COD_DISTRIBUIDORA",
    F.coalesce(F.col("SIG_AGENTE"), F.lit("DESCONHECIDA")),
)

# --- Derivação: tipo de consumidor (regra de negócio central do projeto) ---
df = df.withColumn(
    "TIPO_CONSUMIDOR",
    F.when(F.lower(F.col("DSC_CLASSE_CONSUMO")) == "rural", "Rural")
    .when(F.col("DSC_MODALIDADE").isin("Geração na própria UC", "Autoconsumo remoto"), "Unifamiliar")
    .when(
        F.col("DSC_MODALIDADE").isin(
            "Geração compartilhada", "Empreendimento com múltiplas unidades consumidoras"
        ),
        "Multifamiliar",
    )
    .otherwise("Outro"),
)

df = df.withColumn("ANO", F.year(F.to_date("DTH_ATUALIZACAOCADASTRAL")))

# --- Filtro temporal do projeto: recorte 2020-2025 definido com o orientador ---
df = df.filter((F.col("ANO") >= 2020) & (F.col("ANO") <= 2025))

(
    df.write.mode("overwrite")
    .partitionBy("ANO", "SIG_UF")
    .parquet(args["S3_TRUSTED_PATH"])
)

job.commit()
