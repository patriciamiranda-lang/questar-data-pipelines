# Refresh Materialized Views Lambda (Prod)

## Nome da função (AWS)
lambda-refresh-mv-prod

## Pasta no repositório
lambda/refresh_views

## Objetivo
Atualizar (refresh) todas as Materialized Views do Data Warehouse no Amazon Redshift Serverless, executando a stored procedure:

- `CALL admin.refresh_all_mvs();`

Essa Lambda é invocada após a carga do DW ( `lambda-refresh-dw-prod`).

## O que esta Lambda faz
1. Executa a query SQL no Redshift Data API:
   - `CALL admin.refresh_all_mvs();`
2. Faz polling do status da execução via `describe_statement`.
3. Retorna sucesso quando o status for `FINISHED`.
4. Lança erro se o status for `FAILED` ou `ABORTED`.

## Configurações no código
- Workgroup Redshift Serverless: `default-workgroup`
- Database: `dev`
- Região: `us-east-2`

## Entradas
- Event: qualquer payload (manual/outra Lambda/etc.). O evento não é usado na lógica.

## Saídas
Retorna JSON com:
- `status`: "SUCCESS"
- `statement_id`: ID da execução no Redshift Data API

## Trigger
- É executado após a finalização com sucesso do lambda lambda-refresh-dw-prod.
- Após a Finalizaçação do lambda-refresh-dw-prod o lambda lambda-refresh-mv-prod executada uma procedure no aws Redshift `CALL admin.refresh_all_mvs();` que vai atualizar todas as views materializas no DW.
- Também pode ser executada manualmente ou via agendamento (EventBridge), se desejado.

## Dependências no Redshift
A stored procedure abaixo deve existir no banco:
- `admin.refresh_all_mvs()`

## Permissões IAM necessárias (mínimo)
Para esta Lambda:
- `redshift-data:ExecuteStatement`
- `redshift-data:DescribeStatement`

## Observações importantes
- O polling atual espera somente o status `FINISHED`.
- Caso a procedure seja longa, considere ajustar o `sleep` (atualmente 5s) e/ou adicionar timeout/lógica de logging.
