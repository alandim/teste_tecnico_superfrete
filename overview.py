from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.quicksight import QuickSightCreateIngestionOperator
from airflow.providers.amazon.aws.hooks.kinesis import KinesisHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

# Configurações padrão da DAG
default_args = {
    'owner': 'André',
    'start_date': datetime(2024, 8, 13),
    'retries': 1,
}

# Função para determinar a origem dos dados
def check_data_source(**kwargs):
    # Verificar a existência de dados no S3 ou streaming via Kinesis
    data_source = kwargs.get('data_source', 's3')  # Valor padrão é 's3'
    if data_source == 'streaming':
        return 'process_streaming_data'
    else:
        return 'upload_to_s3'

# Função para processar dados de streaming via Kinesis
def process_streaming_data(**kwargs):
    kinesis = KinesisHook(aws_conn_id='aws_default')
    records = kinesis.get_records(stream_name='your_kinesis_stream')
    # Processar registros de Kinesis e salvar no S3
    with open('/tmp/processed_kinesis_data.txt', 'w') as file:
        for record in records:
            file.write(record['Data'] + '\n')
    # Subir o arquivo processado no S3

with DAG('data_pipeline_with_quicksight', default_args=default_args, schedule_interval='@daily') as dag:

    # Tarefa 1: Verificar a origem dos dados
    determine_data_source = BranchPythonOperator(
        task_id='determine_data_source',
        python_callable=check_data_source,
        provide_context=True
    )

    # Tarefa 2: Ingestão de dados no S3 (se for a origem principal)
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_to_s3',
        filename='/path/to/local/file',
        dest_key='s3_key',
        dest_bucket='your_bucket',
        aws_conn_id='aws_default'
    )

    # Tarefa 3: Processamento de dados em tempo real com Kinesis (se for streaming)
    stream_kinesis = PythonOperator(
        task_id='process_streaming_data',
        python_callable=process_streaming_data
    )

    # Tarefa 4: Executar job ETL no Glue
    run_glue_job = AwsGlueJobOperator(
        task_id='run_glue_job',
        job_name='your_glue_job',
        script_location='s3://your_bucket/scripts/glue_script.py',
        aws_conn_id='aws_default',
    )

    # Tarefa 5: Carregar dados processados no Redshift
    load_to_redshift = S3ToRedshiftOperator(
        task_id='load_to_redshift',
        schema='your_schema',
        table='your_table',
        s3_bucket='your_bucket',
        s3_key='s3_key',
        copy_options=['CSV'],
        aws_conn_id='aws_default',
    )

    # Tarefa 6: Criar nova ingestão de dados no QuickSight para visualização
    create_quicksight_ingestion = QuickSightCreateIngestionOperator(
        task_id='create_quicksight_ingestion',
        data_set_id='your_dataset_id',
        ingestion_id='your_ingestion_id',
        aws_conn_id='aws_default'
    )

    # Tarefa 7: Dummy task para finalização da DAG
    end_pipeline = DummyOperator(
        task_id='end_pipeline'
    )

    # Definindo a sequência das tarefas
    determine_data_source >> [upload_to_s3, stream_kinesis] 
    [upload_to_s3, stream_kinesis] >> run_glue_job >> load_to_redshift >> create_quicksight_ingestion >> end_pipeline

