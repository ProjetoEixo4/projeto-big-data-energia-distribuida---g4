"""
Carga — data/trusted/*.csv -> RDS PostgreSQL


Uso:
    python etl/load_postgres.py
"""
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

TRUSTED_DIR = Path("data/trusted")


def conectar():
    load_dotenv()
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME", "postgres")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    faltando = [k for k, v in {"DB_HOST": host, "DB_USER": user, "DB_PASSWORD": password}.items() if not v]
    if faltando:
        print(f"[load_postgres] ERRO: variáveis de ambiente faltando: {faltando}. "
              "Crie um arquivo .env na raiz do repo (veja o cabeçalho deste script).", file=sys.stderr)
        sys.exit(1)

    print(f"[load_postgres] conectando em {host}:{port}/{dbname} como {user} ...")
    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password,
        sslmode="require",
    )
    conn.autocommit = False
    return conn


def upsert(conn, tabela: str, df: pd.DataFrame, colunas_pk: list[str]) -> None:
    if df.empty:
        print(f"[load_postgres]   {tabela}: nada para carregar (DataFrame vazio)")
        return

    colunas = list(df.columns)
    colunas_update = [c for c in colunas if c not in colunas_pk]

    sql_colunas = ", ".join(colunas)
    sql_pk = ", ".join(colunas_pk)
    sql_update = ", ".join(f"{c} = EXCLUDED.{c}" for c in colunas_update) or "id_tempo = EXCLUDED.id_tempo"

    sql = f"""
        INSERT INTO {tabela} ({sql_colunas})
        VALUES %s
        ON CONFLICT ({sql_pk}) DO UPDATE SET {sql_update}
    """

    valores = [tuple(row) for row in df.where(pd.notnull(df), None).itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, valores, page_size=1000)
    conn.commit()
    print(f"[load_postgres]   {tabela}: {len(df):,} linhas carregadas (upsert)")


def carregar_dim_modalidade(conn) -> dict:
    """Resolve a chave artificial SERIAL: insere combinações novas, devolve
    {(modalidade_aneel, classe_consumo, tipo_consumidor): id_modalidade}."""
    caminho = TRUSTED_DIR / "dim_modalidade.csv"
    df = pd.read_csv(caminho)

    with conn.cursor() as cur:
        cur.execute("SELECT id_modalidade, modalidade_aneel, classe_consumo, tipo_consumidor FROM dim_modalidade")
        existentes = cur.fetchall()
    mapa = {(m, c, t): idm for idm, m, c, t in existentes}

    novas = df[~df.apply(
        lambda r: (r["modalidade_aneel"], r["classe_consumo"], r["tipo_consumidor"]) in mapa, axis=1
    )]

    if not novas.empty:
        with conn.cursor() as cur:
            for _, row in novas.iterrows():
                cur.execute(
                    """INSERT INTO dim_modalidade (modalidade_aneel, classe_consumo, tipo_consumidor)
                       VALUES (%s, %s, %s) RETURNING id_modalidade""",
                    (row["modalidade_aneel"], row["classe_consumo"], row["tipo_consumidor"]),
                )
                novo_id = cur.fetchone()[0]
                mapa[(row["modalidade_aneel"], row["classe_consumo"], row["tipo_consumidor"])] = novo_id
        conn.commit()

    print(f"[load_postgres]   dim_modalidade: {len(mapa)} combinações no total ({len(novas)} novas)")
    return mapa


def main() -> None:
    conn = conectar()
    try:
        print("[load_postgres] --- dimensões estáticas ---")
        upsert(conn, "dim_tempo", pd.read_csv(TRUSTED_DIR / "dim_tempo.csv"), ["id_tempo"])
        upsert(conn, "dim_uf", pd.read_csv(TRUSTED_DIR / "dim_uf.csv"), ["cod_uf"])
        upsert(conn, "dim_fonte_energia", pd.read_csv(TRUSTED_DIR / "dim_fonte_energia.csv"), ["cod_fonte"])

        print("[load_postgres] --- dim_distribuidora ---")
        upsert(conn, "dim_distribuidora", pd.read_csv(TRUSTED_DIR / "dim_distribuidora.csv", dtype={"cod_distribuidora": str}), ["cod_distribuidora"])

        print("[load_postgres] --- dim_modalidade (resolução de chave) ---")
        mapa_modalidade = carregar_dim_modalidade(conn)

        print("[load_postgres] --- dim_clima_uf ---")
        upsert(conn, "dim_clima_uf", pd.read_csv(TRUSTED_DIR / "dim_clima_uf.csv"), ["cod_uf", "id_tempo"])

        print("[load_postgres] --- fato_geracao_uf ---")
        fato_geracao = pd.read_csv(TRUSTED_DIR / "fato_geracao_uf.csv")
        modalidade_natural = pd.read_csv(TRUSTED_DIR / "dim_modalidade.csv")
        fato_geracao = fato_geracao.merge(
            modalidade_natural, on="id_modalidade_natural", how="left"
        )
        fato_geracao["id_modalidade"] = fato_geracao.apply(
            lambda r: mapa_modalidade[(r["modalidade_aneel"], r["classe_consumo"], r["tipo_consumidor"])], axis=1
        )
        fato_geracao = fato_geracao[["id_tempo", "cod_uf", "id_modalidade", "potencia_total_kw", "qtd_conexoes", "potencia_media_kw"]]
        upsert(conn, "fato_geracao_uf", fato_geracao, ["id_tempo", "cod_uf", "id_modalidade"])

        print("[load_postgres] --- fato_tarifa ---")
        upsert(conn, "fato_tarifa", pd.read_csv(TRUSTED_DIR / "fato_tarifa.csv"), ["id_tempo", "cod_uf"])

        print("[load_postgres] --- fato_matriz_energetica ---")
        upsert(conn, "fato_matriz_energetica", pd.read_csv(TRUSTED_DIR / "fato_matriz_energetica.csv"), ["id_tempo", "cod_fonte"])

        print("[load_postgres] concluído com sucesso.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
