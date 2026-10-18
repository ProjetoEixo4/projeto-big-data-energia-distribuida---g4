"""Download em streaming com arquivo temporário (.part) e progresso — usado pelos extratores."""
from __future__ import annotations

from pathlib import Path

import requests

CHUNK = 1024 * 1024


def baixar(url: str, destino: Path, rotulo: str, timeout=(15, 300)) -> Path:
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(destino.suffix + ".part")
    print(f"[{rotulo}] baixando {url}")
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        lido = 0
        with open(tmp, "wb") as f:
            for parte in r.iter_content(CHUNK):
                if parte:
                    f.write(parte)
                    lido += len(parte)
                    if total:
                        print(f"\r[{rotulo}] {100*lido/total:5.1f}% ({lido/1e6:,.0f}/{total/1e6:,.0f} MB)", end="", flush=True)
    print()
    tmp.replace(destino)
    print(f"[{rotulo}] salvo em {destino} ({destino.stat().st_size/1e6:.1f} MB)")
    return destino
