"""
Prova de equivalência: o job PySpark (Glue) e o script pandas produzem a MESMA
fato_geracao_uf. Rodado sobre os dados reais (data/raw + data/trusted) quando
existem; caso contrário é ignorado.

    python tests/test_equivalencia_spark.py      (precisa de Java + pyspark)
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))
sys.path.insert(0, str(RAIZ / "glue"))


def sintetico() -> None:
    """Equivalência no caminho COM DatConexao (arquivo técnico), que os dados reais ainda não exercitam."""
    import os
    import glue_job_gd_pyspark as job
    from pyspark.sql import SparkSession

    base = Path(tempfile.mkdtemp())
    os.environ["OET_DATA_DIR"] = str(base)
    from oet import io as oio
    from transform import seed_dimensoes, transform_geracao_uf as ger

    rng = np.random.default_rng(7)
    n = 3000
    ufs = np.array(["MG", "SP", "BA", "RS"])
    datas = pd.to_datetime("2012-01-01") + pd.to_timedelta(rng.integers(0, 365 * 14, n), unit="D")
    gd = pd.DataFrame({
        "NumCNPJDistribuidora": rng.choice([1234567000100, 98765000155, 5555000199], n),
        "NomAgente": rng.choice(["Alfa", "Beta"], n), "SigUF": rng.choice(ufs, n),
        "SigTipoGeracao": rng.choice(["UFV", "UFV", "UFV", "EOL"], n),
        "DthAtualizaCadastralEmpreend": datas.astype(str),
        "DscModalidadeHabilitado": rng.choice(["Geracao na propria UC", "Condomínio", "Auto consumo remoto", None], n),
        "DscClasseConsumo": rng.choice(["Residencial", "Comercial", "Rural", "REBR ", "Industrial"], n),
        "MdaPotenciaInstaladaKW": rng.uniform(1, 80, n).round(2),
        "CodEmpreendimento": [f"GD.{i:06d}" for i in range(n)],
    })
    tec = pd.DataFrame({  # só 60 % das usinas têm DatConexao; algumas inválidas (1900)
        "CodGeracaoDistribuida": gd["CodEmpreendimento"].iloc[: int(n * 0.6)],
        "DatConexao": (pd.to_datetime("2015-01-01") + pd.to_timedelta(rng.integers(0, 365 * 11, int(n * 0.6)), unit="D")),
    })
    tec.loc[tec.index[:50], "DatConexao"] = pd.Timestamp("1900-01-01")
    (base / "raw" / "aneel_gd").mkdir(parents=True); (base / "raw" / "aneel_gd_tecnico").mkdir(parents=True)
    gd.to_parquet(base / "raw" / "aneel_gd" / "gd.parquet", index=False)
    tec.to_parquet(base / "raw" / "aneel_gd_tecnico" / "tec.parquet", index=False)
    oio.salvar_trusted(seed_dimensoes.gerar_dim_modalidade(), "dim_modalidade")
    rel_pd = ger.transformar()

    out = base / "spark"
    (out / "dim_modalidade").mkdir(parents=True)
    (oio.trusted_dir() / "dim_modalidade.parquet").rename(out / "dim_modalidade" / "p.parquet")
    spark = SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions", "4").getOrCreate()
    rel_sp = job.executar(spark, str(base / "raw"), str(out), lambda r: None)

    ch = ["id_tempo", "cod_uf", "id_modalidade"]
    a = oio.ler_trusted("fato_geracao_uf").sort_values(ch).reset_index(drop=True)
    b = pd.read_parquet(out / "fato_geracao_uf").sort_values(ch).reset_index(drop=True)
    assert len(a) == len(b) and (a[ch].to_numpy() == b[ch].to_numpy()).all(), "sintético: chaves diferentes"
    for c in a.columns.difference(ch):
        assert np.allclose(a[c].astype(float), b[c].astype(float), atol=0.011), f"sintético: coluna {c} difere"
    for k in ("pct_usinas_data_conexao", "pct_potencia_data_conexao"):
        assert abs(rel_pd[k] - rel_sp[k]) < 0.05, f"{k}: pandas={rel_pd[k]} spark={rel_sp[k]}"
    assert rel_pd["arquivo_tecnico_presente"] and not rel_pd["serie_aproximada"] is None
    print(f"OK sintético (com DatConexao): {len(a)} linhas; % potência com data de conexão = {rel_pd['pct_potencia_data_conexao']}")


def main() -> int:
    sintetico()
    raw, trusted = RAIZ / "data" / "raw", RAIZ / "data" / "trusted"
    if not (raw / "aneel_gd").exists() or not (trusted / "fato_geracao_uf.parquet").exists():
        print("SKIP: dados reais ausentes"); return 0
    import glue_job_gd_pyspark as job
    from pyspark.sql import SparkSession

    saida = Path(tempfile.mkdtemp())
    # o job lê dim_modalidade como pasta Parquet
    (saida / "dim_modalidade").mkdir()
    pd.read_parquet(trusted / "dim_modalidade.parquet").to_parquet(saida / "dim_modalidade" / "part.parquet", index=False)
    spark = SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions", "8").getOrCreate()
    rel = job.executar(spark, str(raw), str(saida), lambda r: None)

    ch = ["id_tempo", "cod_uf", "id_modalidade"]
    a = pd.read_parquet(trusted / "fato_geracao_uf.parquet").sort_values(ch).reset_index(drop=True)
    b = pd.read_parquet(saida / "fato_geracao_uf").sort_values(ch).reset_index(drop=True)
    assert len(a) == len(b), f"linhas: pandas={len(a)} spark={len(b)}"
    assert (a[ch].to_numpy() == b[ch].to_numpy()).all(), "chaves diferentes"
    for c in a.columns.difference(ch):
        assert np.allclose(a[c].astype(float), b[c].astype(float), rtol=0, atol=0.011), f"coluna {c} difere"
    for nome in ("dim_distribuidora", "dim_distribuidora_uf"):
        x = pd.read_parquet(trusted / f"{nome}.parquet"); y = pd.read_parquet(saida / nome)
        k = list(x.columns)
        x = x.sort_values(k).reset_index(drop=True); y = y[k].sort_values(k).reset_index(drop=True)
        assert x.equals(y), f"{nome} difere"
    print(f"OK: pandas == spark ({len(a):,} linhas fato); relatório spark: {rel}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
