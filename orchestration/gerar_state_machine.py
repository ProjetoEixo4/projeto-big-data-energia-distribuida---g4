"""
Gera orchestration/state_machine.asl.json (Amazon States Language) do pipeline OET.

Mantido como gerador em vez de JSON escrito à mão porque as 6 extrações e os
ramos paralelos repetem o mesmo bloco de Task/Retry — o gerador garante que
todas tenham exatamente a mesma política de tentativas e de captura de erro.

Os marcadores ${...} são substituídos pelo CloudFormation (DefinitionSubstitutions):
    ${JobPython}  job Glue Python Shell genérico (parâmetro --STEP escolhe a etapa)
    ${JobSpark}   job Glue Spark (geração distribuída)
    ${JobLoad}    job Glue Python Shell dentro da VPC (carga no RDS)
    ${RawBucket} ${TrustedBucket} ${TopicArn} ${Region}

Fluxo:
    Extracao (Parallel x6) -> Dimensoes -> Transformacao (Parallel x3) -> Tarifa -> Carga
        -> NotificarSucesso -> Sucesso
    qualquer erro (após Retry) -> NotificarFalha -> Falha
    entrada {"simular_falha": true} -> FalhaSimulada (demonstra Retry + Catch + SNS)

    python orchestration/gerar_state_machine.py
"""
from __future__ import annotations

import json
from pathlib import Path

RETRY = [{
    "ErrorEquals": ["States.TaskFailed"],
    "IntervalSeconds": 60, "MaxAttempts": 2, "BackoffRate": 2.0,
}]
CATCH = [{"ErrorEquals": ["States.ALL"], "ResultPath": "$.erro", "Next": "NotificarFalha"}]
RESULTADO = {"job.$": "$.JobName", "run.$": "$.Id", "estado.$": "$.JobRunState", "segundos.$": "$.ExecutionTime"}


def tarefa_glue(job: str, step: str | None, proximo: str | None = None, caught: bool = False) -> dict:
    args = {
        "--RAW_BUCKET": "${RawBucket}", "--TRUSTED_BUCKET": "${TrustedBucket}",
        "--EXECUCAO_REF.$": "$$.Execution.Name",
    }
    if step:
        args["--STEP"] = step
    t = {
        "Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync",
        "Parameters": {"JobName": job, "Arguments": args},
        "ResultSelector": RESULTADO, "ResultPath": "$.resultado",
        "TimeoutSeconds": 3600, "Retry": RETRY,
    }
    if caught:
        t["Catch"] = CATCH
    if proximo:
        t["Next"] = proximo
    else:
        t["End"] = True
    return t


def paralelo(ramos: list[tuple[str, str | None, str]], proximo: str, resultado: str) -> dict:
    """ramos = [(nome_estado, step, job)]"""
    return {
        "Type": "Parallel", "ResultPath": f"$.{resultado}", "Next": proximo, "Catch": CATCH,
        "Branches": [{"StartAt": nome, "States": {nome: tarefa_glue(job, step)}} for nome, step, job in ramos],
    }


def construir() -> dict:
    py, sp, ld = "${JobPython}", "${JobSpark}", "${JobLoad}"
    estados = {
        "VerificarSimulacao": {
            "Type": "Choice",
            "Choices": [{
                "And": [{"Variable": "$.simular_falha", "IsPresent": True},
                        {"Variable": "$.simular_falha", "BooleanEquals": True}],
                "Next": "FalhaSimulada",
            }],
            "Default": "Extracao",
        },
        "FalhaSimulada": tarefa_glue(py, "falha_simulada", "NotificarSucesso", caught=True),
        "Extracao": paralelo([
            ("ExtrairGD", "extract_aneel_gd", py),
            ("ExtrairGDTecnico", "extract_aneel_gd_tecnico", py),
            ("ExtrairTarifas", "extract_aneel_tarifas", py),
            ("ExtrairINMET", "extract_inmet", py),
            ("ExtrairIBGE", "extract_ibge_contas_regionais", py),
            ("ExtrairIPCA", "extract_ipca", py),
        ], "Dimensoes", "extracao"),
        "Dimensoes": tarefa_glue(py, "seed_dimensoes", "Transformacao", caught=True),
        "Transformacao": paralelo([
            ("TransformarGDSpark", None, sp),
            ("TransformarClima", "transform_clima", py),
            ("TransformarMatriz", "transform_matriz", py),
        ], "Tarifa", "transformacao"),
        "Tarifa": tarefa_glue(py, "transform_tarifa", "Carga", caught=True),
        "Carga": tarefa_glue(ld, "load_postgres", "NotificarSucesso", caught=True),
        "NotificarSucesso": {
            "Type": "Task", "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {
                "TopicArn": "${TopicArn}",
                "Subject": "OET pipeline: execução concluída",
                "Message.$": "States.Format('Execução {} concluída com sucesso. Detalhes: https://${Region}.console.aws.amazon.com/states/home?region=${Region}#/v2/executions/details/{}', $$.Execution.Name, $$.Execution.Id)",
            },
            "ResultPath": None, "Next": "Sucesso",
        },
        "Sucesso": {"Type": "Succeed"},
        "NotificarFalha": {
            "Type": "Task", "Resource": "arn:aws:states:::sns:publish",
            "Parameters": {
                "TopicArn": "${TopicArn}",
                "Subject": "OET pipeline: FALHA na execução",
                "Message.$": "States.Format('Execução {} FALHOU. Erro: {} | Causa: {} | Detalhes: https://${Region}.console.aws.amazon.com/states/home?region=${Region}#/v2/executions/details/{}', $$.Execution.Name, $.erro.Error, $.erro.Cause, $$.Execution.Id)",
            },
            "ResultPath": None, "Next": "Falha",
        },
        "Falha": {"Type": "Fail", "Error": "PipelineOETFalhou", "Cause": "Veja a notificação SNS e o histórico da execução."},
    }
    return {
        "Comment": "Pipeline OET — Observatório de Energia e Transição Digital (Etapa 3)",
        "StartAt": "VerificarSimulacao", "TimeoutSeconds": 14400, "States": estados,
    }


if __name__ == "__main__":
    destino = Path(__file__).with_name("state_machine.asl.json")
    destino.write_text(json.dumps(construir(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"escrito {destino}")
