
from __future__ import annotations

import argparse
import getpass
import ipaddress
import json
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, WaiterError

RAIZ = Path(__file__).resolve().parent.parent
REGIAO = "sa-east-1"
PREFIXO = "projeto-eixo4-g3"
STACK = "oet-pipeline"
RDS_ID = "database-1"
RAW_BUCKET = f"{PREFIXO}-raw"
ARTEFATOS = f"{PREFIXO}-artifacts"
PACOTES = ["oet", "etl", "transform", "sql"]


def cliente(servico: str):
    return boto3.client(servico, region_name=REGIAO)


# --------------------------------------------------------------------------- descoberta
def descobrir_rede() -> dict:
    rds, ec2 = cliente("rds"), cliente("ec2")
    db = rds.describe_db_instances(DBInstanceIdentifier=RDS_ID)["DBInstances"][0]
    vpc_id = db["DBSubnetGroup"]["VpcId"]
    sgs = [g["VpcSecurityGroupId"] for g in db["VpcSecurityGroups"]]
    vpc = ec2.describe_vpcs(VpcIds=[vpc_id])["Vpcs"][0]
    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])["Subnets"]
    rts = ec2.describe_route_tables(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])["RouteTables"]

    def publica(subnet_id: str) -> bool:
        tabelas = [rt for rt in rts if any(a.get("SubnetId") == subnet_id for a in rt["Associations"])]
        if not tabelas:  # sub-rede sem associação explícita usa a tabela principal
            tabelas = [rt for rt in rts if any(a.get("Main") for a in rt["Associations"])]
        return any(r.get("GatewayId", "").startswith("igw-") for rt in tabelas for r in rt["Routes"])

    pubs = [s for s in subnets if publica(s["SubnetId"])]
    if not pubs:
        raise SystemExit("nenhuma sub-rede pública encontrada na VPC do RDS — informe manualmente no console")
    pub = sorted(pubs, key=lambda s: s["AvailabilityZone"])[0]
    return {
        "DbHost": db["Endpoint"]["Address"], "DbPort": str(db["Endpoint"]["Port"]),
        "DbName": db.get("DBName") or "postgres", "DbUser": db["MasterUsername"],
        "VpcId": vpc_id, "RdsSecurityGroupId": sgs[0],
        "PublicSubnetId": pub["SubnetId"], "PrivateSubnetAz": pub["AvailabilityZone"],
        "PrivateSubnetCidr": cidr_livre(vpc["CidrBlock"], [s["CidrBlock"] for s in subnets]),
        "RdsStatus": db["DBInstanceStatus"], "RdsPublico": db["PubliclyAccessible"],
    }


def cidr_livre(cidr_vpc: str, usados: list[str]) -> str:
    """Primeiro /24 livre dentro da VPC, procurando de trás para frente (longe das sub-redes padrão)."""
    rede = ipaddress.ip_network(cidr_vpc)
    ocupados = [ipaddress.ip_network(u) for u in usados]
    candidatos = list(rede.subnets(new_prefix=24))
    for c in reversed(candidatos):
        if not any(c.overlaps(o) for o in ocupados):
            return str(c)
    raise SystemExit("não há /24 livre na VPC")


# --------------------------------------------------------------------------- stack
def montar_template() -> str:
    import subprocess
    subprocess.run([sys.executable, str(RAIZ / "orchestration" / "gerar_state_machine.py")], check=True)
    subprocess.run([sys.executable, str(RAIZ / "infra" / "build_template.py")], check=True)
    return (RAIZ / "infra" / "oet-pipeline.yaml").read_text(encoding="utf-8")


def implantar(args) -> None:
    cf = cliente("cloudformation")
    d = descobrir_rede()
    senha = args.senha or getpass.getpass("Senha do usuário do RDS (não aparece na tela): ")
    parametros = {
        "ProjectPrefix": PREFIXO, "RawBucketName": RAW_BUCKET, "NotificationEmail": args.email,
        "VpcId": d["VpcId"], "PublicSubnetId": d["PublicSubnetId"],
        "PrivateSubnetCidr": d["PrivateSubnetCidr"], "PrivateSubnetAz": d["PrivateSubnetAz"],
        "RdsSecurityGroupId": d["RdsSecurityGroupId"], "DbHost": d["DbHost"], "DbPort": d["DbPort"],
        "DbName": d["DbName"], "DbUser": d["DbUser"], "DbPassword": senha,
        "ScheduleState": "DISABLED",
    }
    corpo = montar_template()
    pars = [{"ParameterKey": k, "ParameterValue": v} for k, v in parametros.items()]
    existe = True
    try:
        cf.describe_stacks(StackName=STACK)
    except ClientError:
        existe = False
    kw = dict(StackName=STACK, TemplateBody=corpo, Parameters=pars,
              Capabilities=["CAPABILITY_NAMED_IAM"], Tags=[{"Key": "projeto", "Value": "oet-eixo4-g3"}])
    try:
        if existe:
            cf.update_stack(**kw)
            espera = "stack_update_complete"
        else:
            cf.create_stack(**kw, OnFailure="ROLLBACK")
            espera = "stack_create_complete"
    except ClientError as e:
        if "No updates are to be performed" in str(e):
            print("Stack já está atualizada.")
            return
        raise
    print(f"{'Atualizando' if existe else 'Criando'} a stack '{STACK}' (NAT Gateway leva ~3 min) ...")
    try:
        cf.get_waiter(espera).wait(StackName=STACK, WaiterConfig={"Delay": 15, "MaxAttempts": 80})
    except WaiterError:
        eventos = cf.describe_stack_events(StackName=STACK)["StackEvents"]
        for ev in eventos[:15]:
            if "FAILED" in ev["ResourceStatus"]:
                print(f"  ✗ {ev['LogicalResourceId']}: {ev.get('ResourceStatusReason')}")
        raise SystemExit("A stack falhou — veja os motivos acima.")
    saidas = {o["OutputKey"]: o["OutputValue"] for o in cf.describe_stacks(StackName=STACK)["Stacks"][0]["Outputs"]}
    print("Pronto. Confirme a inscrição no e-mail do SNS e rode: python infra/deploy_aws.py artefatos")
    print(json.dumps(saidas, indent=2, ensure_ascii=False))


# --------------------------------------------------------------------------- artefatos
def empacotar(destino: Path) -> Path:
    with zipfile.ZipFile(destino, "w", zipfile.ZIP_DEFLATED) as z:
        for pacote in PACOTES:
            for arq in sorted(p for p in (RAIZ / pacote).rglob("*") if p.suffix in (".py", ".sql")):
                z.write(arq, arq.relative_to(RAIZ).as_posix())
        z.write(RAIZ / "glue" / "__init__.py", "glue/__init__.py")
        z.write(RAIZ / "glue" / "runner.py", "glue/runner.py")
    return destino


def artefatos(_args) -> None:
    s3 = cliente("s3")
    pacote = empacotar(Path("oet_pipeline.zip"))
    envios = [(pacote, "libs/oet_pipeline.zip"),
              (RAIZ / "glue" / "run_step.py", "scripts/run_step.py"),
              (RAIZ / "glue" / "glue_job_gd_pyspark.py", "scripts/glue_job_gd_pyspark.py")]
    for origem, chave in envios:
        s3.upload_file(str(origem), ARTEFATOS, chave)
        print(f"  enviado s3://{ARTEFATOS}/{chave} ({origem.stat().st_size/1e3:,.0f} KB)")
    pacote.unlink()
    print("Código publicado. Próximo: python infra/deploy_aws.py executar")


# --------------------------------------------------------------------------- execução
def enviar_raw(args) -> None:
    """Envia uma pasta local ao bucket RAW (usado para o BEN/EPE, que é baixado manualmente) e liga o versionamento."""
    s3 = cliente("s3")
    s3.put_bucket_versioning(Bucket=RAW_BUCKET, VersioningConfiguration={"Status": "Enabled"})
    base = Path(args.origem)
    arquivos = [p for p in base.rglob("*") if p.is_file()]
    if not arquivos:
        raise SystemExit(f"nenhum arquivo em {base}")
    for a in arquivos:
        chave = f"{args.prefixo}/{a.relative_to(base).as_posix()}"
        s3.upload_file(str(a), RAW_BUCKET, chave)
        print(f"  s3://{RAW_BUCKET}/{chave}")
    print(f"{len(arquivos)} arquivo(s) enviados; versionamento do bucket RAW ligado.")


def arn_state_machine() -> str:
    cf = cliente("cloudformation")
    saidas = cf.describe_stacks(StackName=STACK)["Stacks"][0]["Outputs"]
    return next(o["OutputValue"] for o in saidas if o["OutputKey"] == "StateMachineArn")


def executar(args) -> None:
    sfn = cliente("stepfunctions")
    arn = arn_state_machine()
    nome = datetime.now().strftime("manual-%Y%m%d-%H%M%S") + ("-falha" if args.simular_falha else "")
    entrada = {"simular_falha": True} if args.simular_falha else {}
    ex = sfn.start_execution(stateMachineArn=arn, name=nome, input=json.dumps(entrada))["executionArn"]
    print(f"Execução iniciada: {nome}\nAcompanhe no console: "
          f"https://{REGIAO}.console.aws.amazon.com/states/home?region={REGIAO}#/v2/executions/details/{ex}")
    ultimo = None
    while True:
        d = sfn.describe_execution(executionArn=ex)
        if d["status"] != ultimo:
            print(f"  [{datetime.now():%H:%M:%S}] {d['status']}")
            ultimo = d["status"]
        if d["status"] != "RUNNING":
            break
        time.sleep(20)
    if d["status"] == "FAILED":
        print(f"  Erro: {d.get('error')} — {d.get('cause', '')[:400]}")
    print(f"Duração: {(d['stopDate'] - d['startDate']).total_seconds()/60:.1f} min")
    sys.exit(0 if (d["status"] == "SUCCEEDED") != args.simular_falha else 1)


def agenda(args) -> None:
    ev = cliente("events")
    nome = f"{PREFIXO}-agenda-mensal"
    (ev.enable_rule if args.ligar else ev.disable_rule)(Name=nome)
    print(f"Agenda mensal {'LIGADA' if args.ligar else 'DESLIGADA'} ({nome}).")


def destruir(_args) -> None:
    if input("Digite 'remover' para apagar a stack (NAT, jobs, Step Functions...): ").strip() != "remover":
        raise SystemExit("cancelado")
    cliente("cloudformation").delete_stack(StackName=STACK)
    print("Remoção iniciada. Os buckets trusted/artifacts são mantidos de propósito (evidências).")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("descobrir")
    p = sub.add_parser("implantar"); p.add_argument("--email", required=True); p.add_argument("--senha")
    sub.add_parser("artefatos")
    p = sub.add_parser("enviar-raw"); p.add_argument("--origem", required=True); p.add_argument("--prefixo", required=True)
    p = sub.add_parser("executar"); p.add_argument("--simular-falha", action="store_true")
    p = sub.add_parser("agenda"); g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--ligar", action="store_true"); g.add_argument("--desligar", action="store_true")
    sub.add_parser("destruir")
    args = ap.parse_args()
    if args.cmd == "descobrir":
        d = descobrir_rede(); d.pop("DbPassword", None)
        print(json.dumps(d, indent=2, ensure_ascii=False))
    else:
        {"implantar": implantar, "artefatos": artefatos, "enviar-raw": enviar_raw, "executar": executar,
         "agenda": agenda, "destruir": destruir}[args.cmd](args)


if __name__ == "__main__":
    main()
