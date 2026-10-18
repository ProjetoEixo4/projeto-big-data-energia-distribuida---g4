"""
AWS Glue (PySpark) — ANEEL Geração Distribuída -> camada trusted

Mesma regra de negócio de transform/transform_geracao_uf.py (pandas), reescrita em
Spark porque esta é a única fonte realmente volumosa do projeto (4,6 milhões de
usinas; o cadastro cresce todo mês) — é onde processamento distribuído se justifica.
A equivalência com a versão pandas é verificada por tests/test_equivalencia_spark.py.

Entradas  (S3 raw):      aneel_gd/*.parquet, aneel_gd_tecnico/*.parquet (opcional)
Entradas  (S3 trusted):  dim_modalidade/ (gerada por seed_dimensoes)
Saídas    (S3 trusted):  dim_distribuidora/, dim_distribuidora_uf/, fato_geracao_uf/,
                         _qualidade/geracao.json

Parâmetros do job: --RAW_BUCKET, --TRUSTED_BUCKET, --EXECUCAO_REF
Execução local (teste): python glue/glue_job_gd_pyspark.py --RAW_DIR data/raw --TRUSTED_DIR data/trusted
"""
from __future__ import annotations

import json
import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

ANO_INICIO, ANO_FIM = 2020, 2025
N_MESES = (ANO_FIM - ANO_INICIO + 1) * 12
DATA_MIN_VALIDA = "2012-04-17"            # REN 482/2012
DATA_MAX_VALIDA = f"{ANO_FIM}-12-31 23:59:59"

MODALIDADE_NORM = {
    "geracao na propria uc": "Geração na própria UC",
    "auto consumo remoto": "Autoconsumo remoto",
    "autoconsumo remoto": "Autoconsumo remoto",
    "compartilhada": "Compartilhada",
    "condominio": "Condomínio",
    "condomínio": "Condomínio",
}
CLASSE_NORM = {
    "residencial": "Residencial", "rebr": "Residencial baixa renda", "comercial": "Comercial",
    "industrial": "Industrial", "rural": "Rural", "poder público": "Poder Público",
    "serviço público": "Serviço Público", "iluminação pública": "Iluminação pública",
    "consumo próprio": "Consumo próprio",
}


def _mapa(d: dict):
    return F.create_map(*[F.lit(x) for kv in d.items() for x in kv])


def normalizar(col: str, mapa: dict, padrao: str):
    chave = F.lower(F.trim(F.coalesce(F.col(col), F.lit(""))))
    return F.coalesce(_mapa(mapa)[chave], F.lit(padrao))


def ler_solar(spark: SparkSession, caminho_gd: str) -> DataFrame:
    df = spark.read.parquet(caminho_gd).select(
        "NumCNPJDistribuidora", "NomAgente", "SigUF", "SigTipoGeracao",
        "DthAtualizaCadastralEmpreend", "DscModalidadeHabilitado", "DscClasseConsumo",
        "MdaPotenciaInstaladaKW", "CodEmpreendimento")
    return df.filter((F.col("SigTipoGeracao") == "UFV") & F.col("SigUF").isNotNull()
                     & F.col("CodEmpreendimento").isNotNull())


def dims_distribuidora(solar: DataFrame) -> tuple[DataFrame, DataFrame]:
    d = (solar.filter(F.col("NumCNPJDistribuidora").isNotNull())
         .withColumn("cod_distribuidora", F.lpad(F.col("NumCNPJDistribuidora").cast("long").cast("string"), 14, "0")))
    por_uf = (d.groupBy("cod_distribuidora", F.col("SigUF").alias("cod_uf"))
              .agg(F.count(F.lit(1)).alias("qtd_usinas")))
    # nome mais frequente (empate -> menor em ordem alfabética, como pandas .mode().iat[0])
    nomes = d.groupBy("cod_distribuidora", "NomAgente").agg(F.count(F.lit(1)).alias("n"))
    w_nome = Window.partitionBy("cod_distribuidora").orderBy(F.col("n").desc(), F.col("NomAgente").asc())
    nome = (nomes.withColumn("r", F.row_number().over(w_nome)).filter("r = 1")
            .select("cod_distribuidora", F.col("NomAgente").alias("nome_distribuidora")))
    w_uf = Window.partitionBy("cod_distribuidora").orderBy(F.col("qtd_usinas").desc(), F.col("cod_uf").asc())
    principal = (por_uf.withColumn("r", F.row_number().over(w_uf)).filter("r = 1")
                 .select("cod_distribuidora", F.col("cod_uf").alias("cod_uf_principal")))
    dim = nome.join(principal, "cod_distribuidora", "left")
    return dim, por_uf


def mes_de_entrada(solar: DataFrame, tecnico: DataFrame | None) -> DataFrame:
    df = solar.withColumn("dt_cadastro", F.to_timestamp("DthAtualizaCadastralEmpreend"))
    if tecnico is not None:
        df = df.join(tecnico, "CodEmpreendimento", "left")
    else:
        df = df.withColumn("dt_conexao", F.lit(None).cast("timestamp"))
    lo, hi = F.to_timestamp(F.lit(DATA_MIN_VALIDA)), F.to_timestamp(F.lit(DATA_MAX_VALIDA))
    ok_con = F.col("dt_conexao").between(lo, hi)
    ok_cad = F.col("dt_cadastro").between(lo, hi)
    df = df.withColumn("origem_data", F.when(ok_con, "conexao").when(ok_cad, "cadastro").otherwise("invalida"))
    df = df.withColumn("dt_ref", F.when(ok_con, F.col("dt_conexao")).when(ok_cad, F.col("dt_cadastro")))
    df = df.filter(F.col("dt_ref").isNotNull())
    return (df.withColumn("mes_idx", (F.year("dt_ref") - ANO_INICIO) * 12 + F.month("dt_ref") - 1)
            .withColumn("pot", F.coalesce(F.col("MdaPotenciaInstaladaKW").cast("double"), F.lit(0.0))))


def construir_fato(df: DataFrame, dim_mod: DataFrame, spark: SparkSession) -> DataFrame:
    df = (df.withColumn("modalidade_aneel", normalizar("DscModalidadeHabilitado", MODALIDADE_NORM, "Não informado"))
          .withColumn("classe_consumo", normalizar("DscClasseConsumo", CLASSE_NORM, "Outras"))
          .join(F.broadcast(dim_mod.select("id_modalidade", "modalidade_aneel", "classe_consumo")),
                ["modalidade_aneel", "classe_consumo"], "left")
          .withColumnRenamed("SigUF", "cod_uf"))
    chave = ["cod_uf", "id_modalidade"]

    antes = (df.filter("mes_idx < 0").groupBy(*chave)
             .agg(F.sum("pot").alias("pot0"), F.count(F.lit(1)).alias("qtd0")))
    fluxo = (df.filter("mes_idx >= 0").groupBy(*chave, "mes_idx")
             .agg(F.sum("pot").alias("pot_nova"), F.count(F.lit(1)).alias("qtd_nova")))
    meses = spark.range(N_MESES).select(F.col("id").cast("int").alias("mes_idx"))
    grade = (df.select(*chave).distinct().crossJoin(meses)
             .join(fluxo, [*chave, "mes_idx"], "left").join(antes, chave, "left")
             .fillna(0, subset=["pot_nova", "qtd_nova", "pot0", "qtd0"]))

    w = Window.partitionBy(*chave).orderBy("mes_idx").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    grade = (grade
             .withColumn("potencia_acumulada_kw", F.col("pot0") + F.sum("pot_nova").over(w))
             .withColumn("qtd_acumulada", (F.col("qtd0") + F.sum("qtd_nova").over(w)).cast("long"))
             .withColumn("ano", (F.col("mes_idx") / 12).cast("int") + ANO_INICIO)
             .withColumn("mes", F.col("mes_idx") % 12 + 1)
             .withColumn("id_tempo", F.col("ano") * 100 + F.col("mes")))
    fato = (grade.filter("qtd_acumulada > 0")
            .withColumn("potencia_nova_kw", F.round("pot_nova", 2))
            .withColumn("qtd_novas_conexoes", F.col("qtd_nova").cast("long"))
            .withColumn("potencia_media_kw", F.round(F.col("potencia_acumulada_kw") / F.col("qtd_acumulada"), 2))
            .withColumn("potencia_acumulada_kw", F.round("potencia_acumulada_kw", 2))
            .select("id_tempo", "cod_uf", "id_modalidade", "potencia_acumulada_kw", "qtd_acumulada",
                    "potencia_nova_kw", "qtd_novas_conexoes", "potencia_media_kw"))
    return fato


def executar(spark: SparkSession, raw: str, trusted: str, escrever_relatorio) -> dict:
    solar = ler_solar(spark, f"{raw}/aneel_gd/")
    total_ufv = solar.count()

    try:
        tec = spark.read.parquet(f"{raw}/aneel_gd_tecnico/")
        cols = {c.lower(): c for c in tec.columns}
        tecnico = (tec.select(F.col(cols["codgeracaodistribuida"]).alias("CodEmpreendimento"),
                              F.to_timestamp(F.col(cols["datconexao"])).alias("dt_conexao"))
                   .dropna(subset=["CodEmpreendimento"]).dropDuplicates(["CodEmpreendimento"]))
    except Exception as e:  # arquivo técnico ausente é um modo degradado, não um erro
        if "Path does not exist" not in str(e) and "UNABLE_TO_INFER_SCHEMA" not in str(e):
            raise
        print("AVISO: tabela técnica (DatConexao) ausente — série aproximada pela data de atualização cadastral",
              file=sys.stderr)
        tecnico = None

    dim_mod = spark.read.parquet(f"{trusted}/dim_modalidade/")
    dim_dist, dist_uf = dims_distribuidora(solar)
    entrada = mes_de_entrada(solar, tecnico).cache()
    fato = construir_fato(entrada, dim_mod, spark).cache()

    for nome, df in [("dim_distribuidora", dim_dist), ("dim_distribuidora_uf", dist_uf), ("fato_geracao_uf", fato)]:
        df.coalesce(1).write.mode("overwrite").parquet(f"{trusted}/{nome}/")

    ultimo = fato.agg(F.max("id_tempo").alias("m")).first()["m"]
    ag = (fato.filter(F.col("id_tempo") == ultimo)
          .agg(F.sum("potencia_acumulada_kw").alias("kw"), F.sum("qtd_acumulada").alias("n")).first())
    por_origem = {r["origem_data"]: r["n"] for r in entrada.groupBy("origem_data").count()
                  .withColumnRenamed("count", "n").collect()}
    n_com_data = sum(por_origem.values())
    pot_total = entrada.agg(F.sum("pot")).first()[0] or 0.0
    pot_con = (entrada.filter("origem_data = 'conexao'").agg(F.sum("pot")).first()[0] or 0.0)
    rel = {
        "motor": "spark", "usinas_ufv": total_ufv, "linhas_fato": fato.count(),
        "ufs": fato.select("cod_uf").distinct().count(), "ultimo_id_tempo": int(ultimo),
        "potencia_acumulada_final_mw": round(ag["kw"] / 1000, 1), "qtd_acumulada_final": int(ag["n"]),
        "usinas_com_data": n_com_data,
        "pct_usinas_data_conexao": round(100 * por_origem.get("conexao", 0) / n_com_data, 2) if n_com_data else 0.0,
        "pct_potencia_data_conexao": round(100 * pot_con / pot_total, 2) if pot_total else 0.0,
        "arquivo_tecnico_presente": tecnico is not None,
    }
    rel["serie_aproximada"] = rel["pct_potencia_data_conexao"] < 90.0
    escrever_relatorio(rel)
    return rel


# ----------------------------------------------------------------------------- entrypoints
def _arg(nome: str, padrao: str | None = None) -> str | None:
    if f"--{nome}" in sys.argv:
        return sys.argv[sys.argv.index(f"--{nome}") + 1]
    return padrao


def main() -> None:
    execucao = _arg("EXECUCAO_REF", datetime.utcnow().strftime("manual-%Y%m%dT%H%M%S"))
    if _arg("RAW_DIR"):  # modo local
        raw, trusted = _arg("RAW_DIR"), _arg("TRUSTED_DIR")
        spark = SparkSession.builder.master("local[*]").appName("oet-gd-local").getOrCreate()

        def escrever(rel):
            import os
            os.makedirs(trusted, exist_ok=True)
            with open(f"{trusted}/_qualidade_geracao_spark.json", "w", encoding="utf-8") as f:
                json.dump(rel, f, ensure_ascii=False, indent=2)
    else:  # AWS Glue
        import boto3
        from awsglue.context import GlueContext
        from pyspark.context import SparkContext
        sc = SparkContext.getOrCreate()
        spark = GlueContext(sc).spark_session
        raw, trusted = f"s3://{_arg('RAW_BUCKET')}", f"s3://{_arg('TRUSTED_BUCKET')}"

        def escrever(rel):
            boto3.client("s3").put_object(
                Bucket=_arg("TRUSTED_BUCKET"), Key=f"_qualidade/{execucao}/geracao.json",
                Body=json.dumps(rel, ensure_ascii=False, indent=2).encode("utf-8"))

    rel = executar(spark, raw, trusted, escrever)
    print("RELATORIO " + json.dumps(rel, ensure_ascii=False))


if __name__ == "__main__":
    main()
