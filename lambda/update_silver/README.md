# Refresh DW Lambda (Prod)

## Nome da função (AWS)
lambda-refresh-dw-prod

## Pasta no repositório
lambda/update_silver

> Observação: O Lambda é responsável pelo refresh/carga do DW no Redshift.

## Objetivo
Recarregar as tabelas do schema `dw_redshift` no Amazon Redshift Serverless, realizando:
- `DELETE` nas tabelas de destino
- `INSERT` a partir do schema `dw` (origem)
Ao final, dispara de forma assíncrona a Lambda `lambda-refresh-mv-prod` para atualizar as Materialized Views.

## O que esta Lambda faz 
1. Para cada tabela listada em `LOAD_SQL`:
   - executa `DELETE FROM dw_redshift.<tabela>`
   - executa `INSERT INTO dw_redshift.<tabela> SELECT ... FROM dw.<tabela>`
2. Invoca a Lambda `lambda-refresh-mv-prod` com `InvocationType="Event"` (assíncrono).

## Configurações no código
- Workgroup Redshift Serverless: `default-workgroup`
- Database: `dev`
- Schema destino (DW): `dw_redshift`
- Região: `us-east-2`

## Entradas
- Event: qualquer payload (manual/EventBridge/etc.). O evento é logado, mas não usado na lógica.

## Saídas
Retorna JSON com:
- `status`: SUCCESS
- `run_id`: request id da Lambda
- `tables_updated`: lista de tabelas atualizadas
- `mv_refresh`: TRIGGERED

## Trigger 
- (Preencher conforme sua AWS: manual / EventBridge schedule / Step Functions, etc.)

## Permissões IAM necessárias (mínimo)
Para esta Lambda:
- `redshift-data:ExecuteStatement`
- `redshift-data:DescribeStatement`
- `lambda:InvokeFunction` (para invocar `lambda-refresh-mv-prod`)

E acesso ao Redshift Serverless/Data API conforme o modelo de autenticação adotado (IAM role / permissões de workgroup).

## Observações importantes
- A estratégia atual é **full refresh** (DELETE + INSERT) para todas as tabelas.
- Se for necessário histórico/incremental, essa lógica deverá ser versionada e alterada com cuidado.
