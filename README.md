
### **Explicação da DAG:**
- **determine_data_source:** Decide a origem dos dados, se será do S3 ou streaming via Kinesis.
- **upload_to_s3:** Se a origem for S3, carrega os dados no bucket S3.
- **stream_kinesis:** Se a origem for streaming, processa os dados via Kinesis e os carrega no S3.
- **run_glue_job:** Executa um job ETL no Glue para transformar e preparar os dados.
- **load_to_redshift:** Carrega os dados processados no Redshift para análise.
- **create_quicksight_ingestion:** Cria uma nova ingestão de dados no QuickSight para visualização.
- **end_pipeline:** Finaliza o pipeline.
