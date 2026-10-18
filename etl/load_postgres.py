"""
Carga — camada trusted (Parquet) -> RDS PostgreSQL, atômica e idempotente.

Como funciona, para cada tabela (na ordem das chaves estrangeiras):
    1. VALIDA o Parquet antes de tocar no banco: não vazio, colunas esperadas,
       chave primária sem duplicata, chaves estrangeiras existentes nas
       dimensões que serão carregadas. Falha cedo com mensagem legível.
    2. Cria uma tabela temporária (staging) e carrega o lote inteiro com COPY
       (ordens de grandeza mais rápido que INSERT linha a linha).
    3. Faz UPSERT do staging na tabela final (INSERT ... ON CONFLICT DO UPDATE),
       gravando apenas linhas novas ou que de fato mudaram
       (IS DISTINCT FROM) e contando inseridas x atualizadas.
    4. Registra a contagem em etl_execucao (trilha de auditoria).
Tudo em UMA transação: se qualquer etapa falhar, nada é gravado (rollback).
Rodar de novo com os mesmos dados não altera o banco (0 inseridas, 0 atualizadas).

Credenciais, por ordem de prioridade:
    - DB_SECRET_ARN: segredo no AWS Secrets Manager (modo Glue/Step Functions);
    - variáveis DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD (ou arquivo .env local).
Nunca coloque a senha no código nem no Git.

Uso:
    python etl/load_postgres.py [--execucao-ref NOME] [--ddl | --somente-ddl]
        --ddl          prepara o esquema (sql/00 migração segura + sql/01) e depois carrega
        --somente-ddl  só prepara o esquema
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from oet.io import ler_trusted  # noqa: E402

# tabela -> (colunas na ordem de carga, chave primária, [(coluna_fk, tabela_pai, coluna_pai)])
ESQUEMA = {
    "dim_tempo": (["id_tempo", "ano", "mes", "trimestre"], ["id_tempo"], []),
    "dim_uf": (["cod_uf", "nome_uf", "regiao", "pib_per_capita", "pib_projetado", "ano_ref_pib",
                "populacao", "area_km2"], ["cod_uf"], []),
    "dim_fonte_energia": (["cod_fonte", "nome_fonte", "categoria"], ["cod_fonte"], []),
    "dim_modalidade": (["id_modalidade", "modalidade_aneel", "classe_consumo", "tipo_consumidor"],
                       ["id_modalidade"], []),
    "dim_distribuidora": (["cod_distribuidora", "nome_distribuidora", "cod_uf_principal"],
                          ["cod_distribuidora"], [("cod_uf_principal", "dim_uf", "cod_uf")]),
    "dim_clima_uf": (["cod_uf", "id_tempo", "irradiacao_kwh_m2_dia", "temperatura_media_c",
                      "precipitacao_total_mm", "cobertura_pct", "qtd_estacoes", "amostra_reduzida"],
                     ["cod_uf", "id_tempo"],
                     [("cod_uf", "dim_uf", "cod_uf"), ("id_tempo", "dim_tempo", "id_tempo")]),
    "fato_geracao_uf": (["id_tempo", "cod_uf", "id_modalidade", "potencia_acumulada_kw", "qtd_acumulada",
                         "potencia_nova_kw", "qtd_novas_conexoes", "potencia_media_kw"],
                        ["id_tempo", "cod_uf", "id_modalidade"],
                        [("id_tempo", "dim_tempo", "id_tempo"), ("cod_uf", "dim_uf", "cod_uf"),
                         ("id_modalidade", "dim_modalidade", "id_modalidade")]),
    "fato_tarifa": (["id_tempo", "cod_uf", "tarifa_media_reais_kwh", "tarifa_deflacionada", "qtd_distribuidoras"],
                    ["id_tempo", "cod_uf"],
                    [("id_tempo", "dim_tempo", "id_tempo"), ("cod_uf", "dim_uf", "cod_uf")]),
    "fato_matriz_energetica": (["id_tempo", "cod_fonte", "participacao_percentual", "geracao_gwh",
                                "geracao_servico_publico_gwh", "geracao_autoprodutoras_gwh"],
                               ["id_tempo", "cod_fonte"],
                               [("id_tempo", "dim_tempo", "id_tempo"), ("cod_fonte", "dim_fonte_energia", "cod_fonte")]),
}
ORDEM = list(ESQUEMA)
SCRIPTS_SQL = ["00_migracao_etapa2_para_etapa3.sql", "01_ddl_modelo_dimensional.sql"]


class ErroValidacao(Exception):
    pass


# --------------------------------------------------------------------------- conexão
def credenciais() -> dict:
    arn = os.getenv("DB_SECRET_ARN")
    if arn:
        import boto3  # só no modo AWS
        seg = json.loads(boto3.client("secretsmanager").get_secret_value(SecretId=arn)["SecretString"])
        return {"host": seg["host"], "port": int(seg.get("port", 5432)), "dbname": seg.get("dbname", "postgres"),
                "user": seg["username"], "password": seg["password"]}
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    cfg = {"host": os.getenv("DB_HOST"), "port": int(os.getenv("DB_PORT", "5432")),
           "dbname": os.getenv("DB_NAME", "postgres"), "user": os.getenv("DB_USER"),
           "password": os.getenv("DB_PASSWORD")}
    faltando = [k for k in ("host", "user", "password") if not cfg[k]]
    if faltando:
        raise ErroValidacao(f"credenciais ausentes: {faltando} (use DB_SECRET_ARN ou o arquivo .env)")
    return cfg


def conectar():
    cfg = credenciais()
    print(f"[load_postgres] conectando em {cfg['host']}:{cfg['port']}/{cfg['dbname']} como {cfg['user']}")
    conn = psycopg2.connect(sslmode=os.getenv("DB_SSLMODE", "require"), connect_timeout=15, **cfg)
    conn.autocommit = False
    return conn


# --------------------------------------------------------------------------- validação
def validar(tabelas: dict[str, pd.DataFrame]) -> None:
    erros = []
    for nome, df in tabelas.items():
        colunas, pk, fks = ESQUEMA[nome]
        if df.empty:
            erros.append(f"{nome}: vazio")
            continue
        ausentes = [c for c in colunas if c not in df.columns]
        if ausentes:
            erros.append(f"{nome}: colunas ausentes {ausentes}")
            continue
        dup = int(df.duplicated(pk).sum())
        if dup:
            erros.append(f"{nome}: {dup} linhas com chave primária {pk} duplicada")
        nulos_pk = int(df[pk].isna().any(axis=1).sum())
        if nulos_pk:
            erros.append(f"{nome}: {nulos_pk} linhas com chave primária nula")
        for col, pai, col_pai in fks:
            if pai not in tabelas:
                continue
            valores = df[col].dropna()
            orfas = set(valores) - set(tabelas[pai][col_pai])
            if orfas:
                erros.append(f"{nome}.{col}: {len(orfas)} valores sem pai em {pai} (ex.: {sorted(orfas)[:3]})")
    if erros:
        raise ErroValidacao("validação falhou:\n  - " + "\n  - ".join(erros))


# --------------------------------------------------------------------------- carga
def preparar(df: pd.DataFrame, colunas: list[str]) -> pd.DataFrame:
    out = df[colunas].copy()
    for c in out.columns:
        if str(out[c].dtype) in ("Int64", "Int32", "Int16"):
            out[c] = out[c].astype("Int64")
    return out


def upsert_staging(cur, tabela: str, df: pd.DataFrame) -> tuple[int, int]:
    colunas, pk, _ = ESQUEMA[tabela]
    df = preparar(df, colunas)
    stg = f"stg_{tabela}"
    cur.execute(f"CREATE TEMP TABLE {stg} ON COMMIT DROP AS SELECT {', '.join(colunas)} FROM {tabela} WITH NO DATA")
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    cur.copy_expert(f"COPY {stg} ({', '.join(colunas)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)

    atualiz = [c for c in colunas if c not in pk]
    if atualiz:
        set_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in atualiz)
        distinto = (f"WHERE ({', '.join(f'{tabela}.{c}' for c in atualiz)}) "
                    f"IS DISTINCT FROM ({', '.join(f'EXCLUDED.{c}' for c in atualiz)})")
        acao = f"DO UPDATE SET {set_sql} {distinto}"
    else:
        acao = "DO NOTHING"
    cur.execute(f"""
        INSERT INTO {tabela} ({', '.join(colunas)})
        SELECT {', '.join(colunas)} FROM {stg}
        ON CONFLICT ({', '.join(pk)}) {acao}
        RETURNING (xmax = 0) AS inserida
    """)
    flags = [r[0] for r in cur.fetchall()]
    ins = sum(1 for f in flags if f)
    return ins, len(flags) - ins


def ler_sql(nome: str) -> str:
    """Lê sql/<nome> como recurso do pacote — funciona no repositório e dentro do zip do Glue."""
    from importlib import resources
    return resources.files("sql").joinpath(nome).read_text(encoding="utf-8")


def aplicar_ddl(conn) -> None:
    """Prepara o esquema. A migração 00 (que apaga tabelas de fatos antigas) só roda se o banco
    ainda NÃO tem a tabela etl_execucao, marca da versão Etapa 3 — assim nunca apaga dados já migrados."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.etl_execucao') IS NOT NULL")
        ja_migrado = cur.fetchone()[0]
        for nome in SCRIPTS_SQL:
            if nome.startswith("00_") and ja_migrado:
                print(f"[load_postgres] {nome}: esquema já na versão Etapa 3 — migração dispensada")
                continue
            print(f"[load_postgres] executando {nome}")
            cur.execute(ler_sql(nome).replace("BEGIN;", "").replace("COMMIT;", ""))
    conn.commit()


def carregar(conn, execucao_ref: str) -> dict:
    tabelas = {nome: ler_trusted(nome) for nome in ORDEM}
    print("[load_postgres] validando Parquets ...")
    validar(tabelas)
    resumo = {}
    with conn.cursor() as cur:
        for nome in ORDEM:
            ins, upd = upsert_staging(cur, nome, tabelas[nome])
            n = len(tabelas[nome])
            resumo[nome] = {"linhas": n, "inseridas": ins, "atualizadas": upd, "inalteradas": n - ins - upd}
            cur.execute("INSERT INTO etl_execucao (execucao_ref, tabela, linhas, inseridas, atualizadas) "
                        "VALUES (%s,%s,%s,%s,%s)", (execucao_ref, nome, n, ins, upd))
            print(f"[load_postgres]   {nome:<24} {n:>7,} linhas | +{ins:,} novas | ~{upd:,} alteradas | "
                  f"={n - ins - upd:,} iguais")
    conn.commit()
    return resumo


def main() -> int:
    ap = argparse.ArgumentParser(description="Carga trusted -> PostgreSQL")
    ap.add_argument("--execucao-ref", default=os.getenv("EXECUCAO_REF", "manual"))
    ap.add_argument("--ddl", action="store_true", help="aplica sql/00 e sql/01 antes da carga")
    ap.add_argument("--somente-ddl", action="store_true", help="só prepara o esquema e sai")
    args = ap.parse_args()
    try:
        conn = conectar()
    except (ErroValidacao, psycopg2.Error) as e:
        print(f"[load_postgres] ERRO de conexão: {e}", file=sys.stderr)
        return 1
    try:
        if args.ddl or args.somente_ddl:
            aplicar_ddl(conn)
        if args.somente_ddl:
            print("[load_postgres] esquema pronto.")
            return 0
        resumo = carregar(conn, args.execucao_ref)
        print("[load_postgres] RESUMO " + json.dumps(resumo, ensure_ascii=False))
        print("[load_postgres] concluído com sucesso.")
        return 0
    except (ErroValidacao, psycopg2.Error, FileNotFoundError) as e:
        conn.rollback()
        print(f"[load_postgres] ERRO — nada foi gravado (rollback): {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
