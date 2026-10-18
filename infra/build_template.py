"""Injeta orchestration/state_machine.asl.json no template e grava infra/oet-pipeline.yaml."""
from __future__ import annotations

import json
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
asl = json.loads((RAIZ / "orchestration" / "state_machine.asl.json").read_text(encoding="utf-8"))
texto = json.dumps(asl, ensure_ascii=False, indent=2)
bloco = "\n".join(" " * 12 + linha for linha in texto.splitlines())
modelo = (RAIZ / "infra" / "oet-pipeline.template.yaml").read_text(encoding="utf-8")
assert "__ASL__" in modelo
(RAIZ / "infra" / "oet-pipeline.yaml").write_text(modelo.replace("__ASL__", bloco), encoding="utf-8")
print("infra/oet-pipeline.yaml gerado")
