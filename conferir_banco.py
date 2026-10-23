"""Confere o banco depois de uma execução do pipeline: contagem de linhas por tabela
e o histórico de cargas (etl_execucao), pra ver se bateu o esperado e se rodar de novo
não duplicou nada (inseridas/atualizadas baixas ou zero na segunda vez).

Uso: python conferir_banco.py   (rodar da raiz do repo, com o venv ativado)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from etl.load_postgres import conectar, ORDEM  # reusa a mesma lógica de conexão do projeto

with conectar() as conn, conn.cursor() as cur:
    print("=== Contagem de linhas por tabela ===")
    for tabela in ORDEM:
        cur.execute(f"SELECT count(*) FROM {tabela}")
        print(f"  {tabela:28s} {cur.fetchone()[0]:>10,}")

    print("\n=== Histórico de cargas (etl_execucao) ===")
    cur.execute("""
        SELECT execucao_ref, tabela, linhas, inseridas, atualizadas, carregado_em
        FROM etl_execucao
        ORDER BY carregado_em, tabela
    """)
    linhas = cur.fetchall()
    if not linhas:
        print("  (vazio -- load_postgres ainda nao rodou, ou a tabela de auditoria nao existe)")
    else:
        print(f"  {'execucao_ref':28s} {'tabela':24s} {'linhas':>8s} {'inseridas':>10s} {'atualizadas':>11s}  carregado_em")
        for execucao_ref, tabela, linhas_n, ins, atu, quando in linhas:
            print(f"  {execucao_ref:28s} {tabela:24s} {linhas_n:>8,} {ins if ins is not None else '-':>10} "
                  f"{atu if atu is not None else '-':>11}  {quando}")
