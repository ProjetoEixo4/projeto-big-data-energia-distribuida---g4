"""Ponto de entrada do Glue. Antes de importar qualquer coisa, baixamos e extraímos do S3
o zip com o código (etl/, transform/, oet/, oet_runner/), em vez de depender do
--extra-py-files do Glue. No Python Shell, esse mecanismo às vezes não fica pronto a
tempo quando várias execuções do mesmo Job sobem juntas (é o que acontece no nosso
passo de extração, que dispara 6 em paralelo). Por isso o download manual abaixo:
ele é determinístico e não depende desse comportamento.
"""
from __future__ import annotations

import sys
import tempfile
import zipfile
from pathlib import Path

_BUCKET = "projeto-eixo4-g3-artifacts"   # mesmo PREFIXO usado em infra/deploy_aws.py
_CHAVE = "libs/oet_pipeline.zip"


def _garantir_codigo_no_sys_path() -> None:
    destino = Path(tempfile.gettempdir()) / "oet_codigo"
    marcador = destino / "oet_runner" / "runner.py"
    if not marcador.exists():
        import boto3
        destino.mkdir(parents=True, exist_ok=True)
        zip_local = Path(tempfile.gettempdir()) / "oet_pipeline.zip"
        boto3.client("s3").download_file(_BUCKET, _CHAVE, str(zip_local))
        with zipfile.ZipFile(zip_local) as z:
            z.extractall(destino)
    if str(destino) not in sys.path:
        sys.path.insert(0, str(destino))


_garantir_codigo_no_sys_path()

from oet_runner.runner import main  # noqa: E402  (precisa vir depois de garantir o sys.path)

main()
