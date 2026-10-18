"""
Runner dos jobs AWS Glue "Python Shell" do OET.

Um único script, parametrizado por --STEP, executa cada etapa do pipeline com o
MESMO código que roda no computador do aluno (pacotes etl/, transform/, oet/,
enviados ao Glue como --extra-py-files). O runner cuida só da "cola" com a nuvem:

    1. baixa do S3 as entradas que a etapa precisa  (raw e/ou trusted)  -> /tmp/oet/data/...
    2. executa o módulo da etapa (o script original, sem alterações)
    3. envia ao S3 o que a etapa produziu            (raw e/ou trusted + relatórios)
    4. imprime um resumo JSON (vai para o CloudWatch Logs = evidência de execução)

Layout no S3
    s3://<raw>/<fonte>/<arquivos>                       dado bruto, como veio da fonte
    s3://<trusted>/<tabela>/<tabela>.parquet            tabela tratada (Spark: várias partes)
    s3://<trusted>/_qualidade/<execucao>/<nome>.json    relatórios de qualidade por execução

Parâmetros: --STEP <nome>  --RAW_BUCKET <b>  --TRUSTED_BUCKET <b>  [--EXECUCAO_REF <id>]
            [--DB_SECRET_ARN <arn>]  (somente load_postgres)
Qualquer falha -> exceção -> Glue marca o job como FAILED -> Step Functions aciona Retry/Catch.
"""
from __future__ import annotations

import argparse
import json
import os
import runpy
import sys
import time
from pathlib import Path

TABELAS_SPARK = ["dim_distribuidora", "dim_distribuidora_uf", "fato_geracao_uf"]
TABELAS_SEED = ["dim_tempo", "dim_uf", "dim_fonte_energia", "dim_modalidade"]
TABELAS_CARGA = TABELAS_SEED + ["dim_distribuidora", "dim_clima_uf", "fato_geracao_uf",
                                "fato_tarifa", "fato_matriz_energetica"]

# módulo, argumentos, raw que precisa baixar, raw que produz, trusted que precisa, trusted que produz
STEPS: dict[str, dict] = {
    "extract_aneel_gd":              dict(mod="etl.extract_aneel_gd", put_raw=["aneel_gd"]),
    "extract_aneel_gd_tecnico":      dict(mod="etl.extract_aneel_gd_tecnico", put_raw=["aneel_gd_tecnico"]),
    "extract_aneel_tarifas":         dict(mod="etl.extract_aneel_tarifas", put_raw=["aneel_tarifas"]),
    "extract_inmet":                 dict(mod="etl.extract_inmet", argv=["--ano-inicio", "2020", "--ano-fim", "2025"],
                                          put_raw=["inmet"]),
    "extract_ibge_contas_regionais": dict(mod="etl.extract_ibge_contas_regionais", put_raw=["ibge_contas_regionais"]),
    "extract_ipca":                  dict(mod="etl.extract_ipca", put_raw=["ibge_ipca"]),
    "seed_dimensoes":                dict(mod="transform.seed_dimensoes", get_raw=["ibge_contas_regionais"],
                                          put_trusted=TABELAS_SEED),
    "transform_clima":               dict(mod="transform.transform_clima_uf", get_raw=["inmet"],
                                          put_trusted=["dim_clima_uf"]),
    "transform_matriz":              dict(mod="transform.transform_matriz_energetica", get_raw=["epe_ben"],
                                          put_trusted=["fato_matriz_energetica"]),
    "transform_tarifa":              dict(mod="transform.transform_tarifa", get_raw=["aneel_tarifas", "ibge_ipca"],
                                          get_trusted=["dim_distribuidora_uf", "dim_uf"], put_trusted=["fato_tarifa"]),
    "falha_simulada":                dict(mod=None, simula_falha=True),   # demonstra Retry/Catch/SNS
    "load_postgres":                 dict(mod="etl.load_postgres", argv=["--ddl", "--execucao-ref", "{EXECUCAO_REF}"],
                                          get_trusted=TABELAS_CARGA),
}


# ----------------------------------------------------------------------------- S3
def _s3():
    import boto3
    return boto3.client("s3")


def baixar_prefixo(s3, bucket: str, prefixo: str, destino: Path) -> int:
    """Baixa s3://bucket/prefixo/** para destino/** ; devolve nº de objetos."""
    n = 0
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefixo.rstrip("/") + "/"):
        for obj in pagina.get("Contents", []):
            if obj["Key"].endswith("/"):
                continue
            rel = obj["Key"][len(prefixo.rstrip("/")) + 1:]
            alvo = destino / rel
            alvo.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, obj["Key"], str(alvo))
            n += 1
    return n


def enviar_arquivos(s3, bucket: str, base: Path, prefixo_s3: str, arquivos: list[Path]) -> int:
    for a in arquivos:
        s3.upload_file(str(a), bucket, f"{prefixo_s3}/{a.relative_to(base).as_posix()}")
    return len(arquivos)


# ----------------------------------------------------------------------------- execução
def executar_modulo(mod: str, argv: list[str]) -> None:
    antigo = sys.argv
    sys.argv = [mod] + argv
    try:
        runpy.run_module(mod, run_name="__main__")
    except SystemExit as e:
        if e.code not in (0, None):
            raise RuntimeError(f"módulo {mod} terminou com código {e.code}") from None
    finally:
        sys.argv = antigo


def rodar(step: str, raw_bucket: str, trusted_bucket: str, execucao: str, workdir: Path) -> dict:
    spec = STEPS[step]
    if spec.get("simula_falha"):
        raise RuntimeError("FALHA SIMULADA de propósito (entrada {\"simular_falha\": true}) — "
                           "serve para demonstrar Retry, Catch e a notificação SNS")
    t0 = time.time()
    s3 = _s3()
    data = workdir / "data"
    os.makedirs(data / "raw", exist_ok=True)
    os.makedirs(data / "trusted", exist_ok=True)
    os.chdir(workdir)                       # os scripts usam caminhos relativos "data/..."
    os.environ["OET_DATA_DIR"] = str(data)
    os.environ["EXECUCAO_REF"] = execucao

    baixados = {}
    for fonte in spec.get("get_raw", []):
        baixados[f"raw/{fonte}"] = baixar_prefixo(s3, raw_bucket, fonte, data / "raw" / fonte)
    for tabela in spec.get("get_trusted", []):
        baixados[f"trusted/{tabela}"] = baixar_prefixo(s3, trusted_bucket, tabela, data / "trusted" / tabela)
    print(f"[runner] entradas: {json.dumps(baixados)}")

    argv = [a.replace("{EXECUCAO_REF}", execucao) for a in spec.get("argv", [])]
    executar_modulo(spec["mod"], argv)

    enviados = {}
    for fonte in spec.get("put_raw", []):
        pasta = data / "raw" / fonte
        arquivos = sorted(p for p in pasta.rglob("*") if p.is_file() and not p.name.endswith(".part"))
        if not arquivos:
            raise RuntimeError(f"etapa {step} não produziu nada em raw/{fonte}")
        enviados[f"raw/{fonte}"] = enviar_arquivos(s3, raw_bucket, data / "raw", "", arquivos)
    for tabela in spec.get("put_trusted", []):
        arq = data / "trusted" / f"{tabela}.parquet"
        if not arq.exists():
            raise RuntimeError(f"etapa {step} não gerou {arq.name}")
        s3.upload_file(str(arq), trusted_bucket, f"{tabela}/{tabela}.parquet")
        enviados[f"trusted/{tabela}"] = 1
    relatorios = sorted((data / "trusted").glob("_qualidade_*.json")) if spec.get("put_trusted") else []
    for r in relatorios:
        s3.upload_file(str(r), trusted_bucket, f"_qualidade/{execucao}/{r.name[len('_qualidade_'):]}")
        enviados[f"qualidade/{r.name}"] = 1

    resumo = {"step": step, "execucao": execucao, "segundos": round(time.time() - t0, 1),
              "entradas": baixados, "saidas": enviados}
    print("[runner] RESUMO " + json.dumps(resumo, ensure_ascii=False))
    return resumo


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--STEP", required=True, choices=sorted(STEPS))
    ap.add_argument("--RAW_BUCKET", required=True)
    ap.add_argument("--TRUSTED_BUCKET", required=True)
    ap.add_argument("--EXECUCAO_REF", default=time.strftime("manual-%Y%m%dT%H%M%S"))
    ap.add_argument("--WORKDIR", default="/tmp/oet")
    ap.add_argument("--DB_SECRET_ARN", default=None, help="segredo do Secrets Manager com as credenciais do RDS")
    args, _ = ap.parse_known_args(argv)   # Glue injeta argumentos extras (--JOB_NAME etc.)
    if args.DB_SECRET_ARN:
        os.environ["DB_SECRET_ARN"] = args.DB_SECRET_ARN
    rodar(args.STEP, args.RAW_BUCKET, args.TRUSTED_BUCKET, args.EXECUCAO_REF, Path(args.WORKDIR))


if __name__ == "__main__":
    main()
