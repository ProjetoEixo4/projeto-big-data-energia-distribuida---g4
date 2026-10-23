

    python tests/test_pipeline_e2e_moto.py

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))
sys.path.insert(0, str(RAIZ / "glue"))

os.environ.setdefault("AWS_ACCESS_KEY_ID", "teste")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "teste")
os.environ.setdefault("AWS_DEFAULT_REGION", "sa-east-1")

import boto3  # noqa: E402
from moto import mock_aws  # noqa: E402

RAW, TRUSTED = "oet-raw-teste", "oet-trusted-teste"


def semear_raw(s3) -> None:
    for pasta in (RAIZ / "data" / "raw").iterdir():
        for arq in pasta.rglob("*"):
            if arq.is_file():
                s3.upload_file(str(arq.resolve()), RAW, f"{pasta.name}/{arq.relative_to(pasta).as_posix()}")


def spark_gd_para_s3(s3) -> None:
    """Executa o job PySpark em modo local e publica as saídas no S3 como o Glue faria."""
    import glue_job_gd_pyspark as job
    from pyspark.sql import SparkSession
    tmp = Path(tempfile.mkdtemp())
    raw_local = tmp / "raw"; trusted_local = tmp / "trusted"
    (trusted_local / "dim_modalidade").mkdir(parents=True)
    s3.download_file(TRUSTED, "dim_modalidade/dim_modalidade.parquet", str(trusted_local / "dim_modalidade" / "p.parquet"))
    raw_local.mkdir()
    (raw_local / "aneel_gd").symlink_to((RAIZ / "data" / "raw" / "aneel_gd").resolve())
    spark = SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions", "8").getOrCreate()
    job.executar(spark, str(raw_local), str(trusted_local), lambda r: s3.put_object(
        Bucket=TRUSTED, Key="_qualidade/e2e/geracao.json", Body=str(r).encode()))
    for nome in job.__dict__.get("TABELAS", ["dim_distribuidora", "dim_distribuidora_uf", "fato_geracao_uf"]):
        for parte in (trusted_local / nome).glob("*.parquet"):
            s3.upload_file(str(parte), TRUSTED, f"{nome}/{parte.name}")
    shutil.rmtree(tmp, ignore_errors=True)


@mock_aws
def main() -> int:
    from glue import runner
    s3 = boto3.client("s3")
    for b in (RAW, TRUSTED):
        s3.create_bucket(Bucket=b, CreateBucketConfiguration={"LocationConstraint": "sa-east-1"})
    semear_raw(s3)
    work = Path(tempfile.mkdtemp())
    for step in ["seed_dimensoes"]:
        runner.rodar(step, RAW, TRUSTED, "e2e", work / step)
    spark_gd_para_s3(s3)
    for step in ["transform_clima", "transform_matriz", "transform_tarifa", "load_postgres"]:
        runner.rodar(step, RAW, TRUSTED, "e2e", work / step)
    chaves = sorted(o["Key"] for o in s3.list_objects_v2(Bucket=TRUSTED).get("Contents", []))
    print("OBJETOS TRUSTED:", *chaves, sep="\n  ")
    shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
