# IAM (Questar)

## Roles usadas pelo pipeline
- AWSGlueServiceRole-qstr-bronze (Glue jobs Bronze/Ingestion)
- AWSGlueServiceRole-qstr-silver (Glue jobs Silver)
- <outras roles usadas: Redshift / Bedrock / Lambda etc>

## Roles existentes mas NÃO usadas atualmente
- AWSGlueServiceRole-qstr-gold
- AWSGlueServiceRole-qstr-platinum

Motivo: não há jobs/recursos do pipeline apontando para essas roles neste momento.
Se forem ativadas no futuro, exportar e versionar a trust policy + policies anexadas.
