# Bronze Ingestion Lambda

## Nome da função (AWS)
**lambda-etl-orchestrator**

## Objetivo
Esta função Lambda é responsável pela **ingestão dos arquivos brutos na camada Bronze** do Data Lake.

Seu papel é **organizar os dados assim que eles chegam no bucket**, movendo-os da raiz para a pasta `bronze/`, garantindo que apenas arquivos válidos sejam ingeridos.

---

## Gatilho (Trigger)
- Evento do Amazon S3 (`ObjectCreated`)
- Disparado sempre que um arquivo é gravado na raiz do bucket

---

## Comportamento
- Valida se o arquivo pertence aos prefixos permitidos de ingestão
- Ignora arquivos que já estejam na pasta `bronze/`
- Move os arquivos elegíveis da raiz para `bronze/<prefixo_original>/`
- Remove o arquivo da raiz após a cópia bem-sucedida
- Ignora arquivos fora do escopo de ingestão

---

## Prefixos monitorados
A Lambda processa apenas arquivos que iniciam com os seguintes prefixos:

- `QBR_Production_drivers/`
- `QBR_Production_events/`
- `QBR_Production_group/`
- `QBR_Production_schemes/`
- `QBR_Production_trips/`
- `QBR_Production_vehicles/`

---

## Escopo
- ❌ Não realiza transformações
- ❌ Não aplica regras de negócio
- ❌ Não grava dados em tabelas
- ✅ Apenas organiza os arquivos na camada Bronze

---

## Camada do Data Lake
- **Bronze**
- Dados brutos, sem tratamento

---

## Observação importante
O código de execução desta Lambda **está atualmente versionado e executando diretamente na AWS**.

Este repositório representa:
- A **estrutura do projeto**
- O **contrato funcional**
- A **documentação oficial** do pipeline

---

## Dependências
- Amazon S3
- AWS Lambda
- IAM Role com permissões de leitura, escrita e deleção de objetos no bucket
