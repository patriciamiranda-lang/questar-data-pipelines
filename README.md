# Questar Data Pipelines (AWS)

Pipeline de dados para ingestão e processamento de arquivos **QBR_Production** no AWS, organizando os dados em camadas **Bronze** e **Silver**, com consumo via **Athena/Glue Catalog** e **Redshift** (quando aplicável).

---

## O que este projeto faz

1. Recebe arquivos no S3 com padrão:
   - `QBR_Production_YYYY_MM_DD_<tabela>.csv`
2. Move automaticamente para a camada **Bronze** (`bronze/`) e organiza por tabela/data.
3. Processa os dados com **AWS Glue** e salva na camada **Silver** em **Parquet**, particionado por `p_source_date`.
4. Disponibiliza consulta via **Athena** e suporte para materialização/consumo no **Redshift**.

---

## Estrutura do repositório

- `lambda/bronze_ingestion/`  
  Lambda responsável por validar e mover arquivos da raiz do bucket para `bronze/`, evitando loops.

- `lambda/refresh_views/`  
  Rotinas para refresh/recriação de views no Redshift (quando aplicável).

- `lambda/update_silver/`  
  Rotinas auxiliares relacionadas à atualização da camada Silver (quando aplicável).

- `glue/jobs/`  
  Jobs Glue da camada Silver (ETL para Parquet particionado):
  - `silver_drivers`
  - `silver_events`
  - `silver_groups`
  - `silver_schemes`
  - `silver_trips`
  - `silver_vehicles`
  - `silver_checkpoint`

---

## Convenções

### Arquivo de entrada
Formato esperado:
- `QBR_Production_YYYY_MM_DD_<tabela>.csv`

### Partição `p_source_date`
A `p_source_date` é extraída do nome do arquivo, por exemplo:
- `QBR_Production_2025_10_14_events.csv` → `p_source_date = 2025-10-14`

Saída (Silver) em Parquet particionado:
- `silver/<tabela>/p_source_date=YYYY-MM-DD/`

---

## Como usar (alto nível)

1. Suba o arquivo `QBR_Production_*` no bucket S3 (raiz).
2. A Lambda **bronze_ingestion** move e organiza em `bronze/`.
3. Execute o Glue Job correspondente (`silver_events`, `silver_trips`, etc.).
4. Consulte os dados via Athena/Glue Catalog (e Redshift, se estiver configurado).

---

## Observações

- Logs e execuções podem ser acompanhados no **CloudWatch** (Lambda e Glue).
- Caso apareça `__HIVE_DEFAULT_PARTITION__`, normalmente é efeito de execuções antigas com partição nula; garanta sempre `p_source_date` preenchida.

---
