# Guia de implantação da Etapa 3 — passo a passo

Para quem tem acesso de administrador na conta AWS (hoje: Lucas e Guilherme). Tempo total: ~1h30 na primeira vez
(a maior parte é espera). Região: **sa-east-1 (São Paulo)**. Todos os comandos são rodados na raiz do repositório,
no PowerShell, com o ambiente virtual do projeto ativo.

> Tudo aqui foi testado localmente (testes unitários, equivalência pandas × Spark, pipeline inteiro em S3 simulado
> com carga no PostgreSQL 16, validação do template com `cfn-lint` e do fluxo com `asl-validator`). O que **não** pôde
> ser testado fora da AWS real é o comportamento dos serviços em si (Glue, Step Functions). Por isso a primeira
> execução é acompanhada passo a passo e o guia traz a seção "Se algo falhar".

## 0. Antes de começar (5 min)

1. **RDS ligado**: Console AWS → RDS → `database-1` → status *Disponível* e *Acesso público: Sim*. (O RDS parado por mais
   de 7 dias é religado sozinho pela AWS; confira para não pagar à toa.)
2. **Chave de acesso**: Console → IAM → Usuários → seu usuário → *Credenciais de segurança* → *Criar chave de acesso*
   (caso de uso: *CLI*). Guarde as duas chaves.
3. No PowerShell:

```powershell
pip install boto3
$env:AWS_ACCESS_KEY_ID="COLE_AQUI"
$env:AWS_SECRET_ACCESS_KEY="COLE_AQUI"
$env:AWS_DEFAULT_REGION="sa-east-1"
python infra/deploy_aws.py descobrir
```

O último comando mostra o RDS, a VPC, a sub-rede pública e a faixa livre que serão usadas. Se aparecer erro de
permissão, a chave não é de administrador.

## 1. Criar a infraestrutura (10 min)

```powershell
python infra/deploy_aws.py implantar --email SEU_EMAIL@gmail.com
```

Pede a senha do banco (não aparece na tela) e cria, via CloudFormation: buckets *trusted* e *artifacts*, tópico SNS,
segredo com as credenciais do RDS, rede privada com NAT, perfis IAM, os 3 jobs do Glue, o Step Functions e a regra do
EventBridge (**desligada**). O NAT leva uns 3 minutos.

Depois: **abra o e-mail e clique em "Confirm subscription"** (sem isso nenhum aviso chega).

## 2. Enviar o código e o dado manual (5 min)

```powershell
python infra/deploy_aws.py artefatos
python etl/extract_epe_ben.py --pasta "C:\Users\lucas\Downloads\projeto Eixo 4\EPE"
python infra/deploy_aws.py enviar-raw --origem data/raw/epe_ben --prefixo epe_ben
```

O BEN da EPE é o único dado baixado à mão; os outros seis são baixados pelo próprio pipeline.
Sempre que mudar algum `.py`, rode `artefatos` de novo.

## 3. Primeira execução (25–40 min)

```powershell
python infra/deploy_aws.py executar
```

O comando imprime o link da execução no console; abra e acompanhe o desenho do fluxo ficando verde:
*Extração (6 em paralelo) → Dimensões → Transformação (3 em paralelo) → Tarifa → Carga → aviso por e-mail*.
Cada Glue leva 1–2 min só para iniciar; o INMET (550 MB) é o mais demorado.

Ao final deve chegar o e-mail **"OET pipeline: execução concluída"**.

## 4. Conferir o resultado no banco

Qualquer cliente SQL (DBeaver/pgAdmin) no `database-1`:

```sql
SELECT tabela, linhas, inseridas, atualizadas, carregado_em
FROM etl_execucao ORDER BY id_execucao DESC LIMIT 9;           -- auditoria da execução

SELECT count(*) FROM fato_geracao_uf;                           -- ≈ 34 mil
SELECT count(*) FROM fato_tarifa;                               -- 1 944 (27 UFs x 72 meses)
SELECT count(*) FROM dim_clima_uf;                              -- 1 944
SELECT cod_fonte, id_tempo, participacao_percentual
FROM fato_matriz_energetica WHERE cod_fonte = 'SOLAR_FV' ORDER BY id_tempo;   -- 1,71 % (2020) → 11,36 % (2025)
```

Veja também `s3://projeto-eixo4-g3-trusted/_qualidade/<execução>/*.json` — relatórios de qualidade
(em especial `geracao.json`: se `serie_aproximada` for `true`, a série usou a data de cadastro e não a de conexão).

## 5. Provar idempotência e tratamento de falhas (as evidências que o professor quer ver)

**a) Reexecução sem duplicar:**

```powershell
python infra/deploy_aws.py executar
```

Na consulta `etl_execucao`, a nova execução deve mostrar `inseridas = 0` e `atualizadas = 0` em todas as tabelas
(se a ANEEL publicou dados novos no intervalo, aparecerão só as linhas realmente novas).

**b) Falha simulada (Retry + Catch + SNS):**

```powershell
python infra/deploy_aws.py executar --simular-falha
```

O job falha de propósito, o Step Functions tenta de novo 2 vezes (60 s e 120 s de espera), captura o erro e envia o
e-mail **"OET pipeline: FALHA na execução"**. O comando termina com sucesso (a falha era o esperado).

**c) Agenda mensal (EventBridge)** — só depois de (a) e (b) terem passado:

```powershell
python infra/deploy_aws.py agenda --ligar
```

Dispara todo dia 1º às 06:00 de Brasília. Para a apresentação, deixe ligada e mostre a regra no console
(EventBridge → Regras → `projeto-eixo4-g3-agenda-mensal`).

## 6. O que capturar (prints) para o documento da Etapa 3

1. Step Functions: desenho do fluxo com todos os estados verdes (execução com sucesso) e a aba *Histórico de eventos*.
2. Step Functions: execução da falha simulada, mostrando as tentativas (Retry) e o caminho `NotificarFalha → Falha`.
3. Glue → Jobs: os 3 jobs e o histórico de execuções (aba *Runs*), com tempo de cada um.
4. CloudWatch Logs: log do job de carga com a linha `RESUMO` (linhas novas/alteradas/iguais por tabela).
5. S3: bucket *trusted* com as pastas das tabelas em Parquet e a pasta `_qualidade`.
6. E-mails recebidos (sucesso e falha) e a regra do EventBridge.
7. Consulta `etl_execucao` da 1ª e da 2ª execuções (mostrando `inseridas = 0` na 2ª).
8. O repositório no GitHub com a pasta `orchestration/` e `infra/`.

## 7. Encerrar e economizar

* O **NAT Gateway cobra por hora (~US$ 1,5/dia) mesmo parado.** Depois de coletar as evidências:
  `python infra/deploy_aws.py destruir` remove a stack inteira (os buckets *trusted*/*artifacts* ficam, com as
  evidências). Para reimplantar depois, é só repetir os passos 1 e 2.
* Pare o RDS (*Ações → Parar temporariamente*) quando não estiver usando.

## Se algo falhar

| Sintoma | Provável causa | O que fazer |
|---|---|---|
| `implantar` falha em `NatGateway` ou `PrivateSubnet` | faixa de IP em uso ou sub-rede não pública | rode `descobrir`, confirme `PublicSubnetId` e tente de novo |
| Job Glue `FAILED` em `ModuleNotFoundError` | esqueceu `artefatos` | rode `artefatos` e reexecute |
| `pip` falha dentro do job | endereço de pacote indisponível momentaneamente | reexecute; o Retry cobre isso |
| Job de carga: `timeout` conectando no banco | RDS parado ou Security Group | RDS ligado? A regra de entrada 5432 vinda do `…-glue-sg` existe no SG do RDS? |
| Extração `INMET`/`ANEEL` falha | site fora do ar/lento | reexecute mais tarde (o Retry já tenta 3 vezes) |
| `FalhaSimulada` não dispara | entrada JSON errada | use exatamente `--simular-falha` |

Onde ver o erro real: Glue → Jobs → *Runs* → *Error logs* (CloudWatch). Se não resolver, mande a mensagem do erro
(as 15 últimas linhas) para quem mantém o código.
