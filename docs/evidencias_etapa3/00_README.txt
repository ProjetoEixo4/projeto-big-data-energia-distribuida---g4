Evidências da Etapa 3 — primeira execução real do pipeline na AWS (22/10/2026)
=================================================================================

Ordem cronológica da depuração até a execução final funcionando, mais as 3
provas que a etapa pede (sucesso, falha forçada, repetição sem duplicar).
Usar junto com o texto já redigido da seção "Etapa 3" do documento.

DEPURAÇÃO (3 bugs encontrados e corrigidos — ver seção "Desafios e correções"
do texto da Etapa 3 pra explicação completa de cada um):

  01_terminal_primeira_falha.png           primeira tentativa falhando (terminal)
  02_graph_falha_pacote_glue.png           Step Functions: 3 jobs em vermelho
  03_glue_console_modulenotfounderror.png  console do Glue mostrando o erro
  04_glue_console_argumentos_entrada.png   confirma --extra-py-files configurado
  05_glue_console_python_library_path.png  confirma caminho do zip configurado
  06_cloudwatch_traceback_completo.png     traceback completo do Python (bug 1)
  07_graph_falha_race_condition.png        ainda falhando após 1ª correção (bug 2)
  08_graph_falha_bug_chave_s3.png          extração ok, transformação clima falha (bug 3)

PROVA 1 — EXECUÇÃO COM SUCESSO:

  09_graph_execucao_completa_rodando.png   fluxo quase todo verde, em andamento
  10_terminal_primeira_execucao_sucesso.png  terminal: SUCCEEDED, 10.9 min
  12_execucao_2_sucesso.txt                segunda execução, também SUCCEEDED (12.0 min)
                                            — confirma que não foi coincidência

PROVA 2 — FALHA FORÇADA (Retry + Catch + notificação):

  13_execucao_3_falha_simulada.txt         terminal: FAILED com --simular-falha
  11_email_notificacoes_sucesso_e_falha.png  caixa de entrada com os e-mails do SNS

PROVA 3 — REPETIÇÃO SEM DUPLICAR DADOS:

  14_conferencia_banco.txt                 contagem de linhas + histórico de cargas
                                            (etl_execucao) comparando as 2 execuções
                                            de sucesso

Execuções de referência no Step Functions (nomes usados no texto):
  manual-20261022-202547          1ª execução de sucesso (10.9 min)
  manual-20261022-203915          2ª execução de sucesso (12.0 min)
  manual-20261022-205808-falha    execução com falha simulada (6.1 min)
