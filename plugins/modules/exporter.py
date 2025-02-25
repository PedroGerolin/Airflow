from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

class Exporter:

    def __init__(self, project_id,dataset_id,table_name):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_name = table_name

    def export(self, context):

        project_id = self.project_id
        dataset_id = self.dataset_id
        table_name = self.table_name
        bucket_name = 'gerolin_etl'
        
        export_from_bq_to_gcs = BigQueryToGCSOperator(
            task_id='export_from_bq_to_gcs',
            source_project_dataset_table=f'{project_id}.{dataset_id}.{table_name}',
            destination_cloud_storage_uris=[f'gs://{bucket_name}/{table_name}'],
            export_format='parquet',
            print_header=True,
            gcp_conn_id ='google_cloud_default'
        )
        export_from_bq_to_gcs.execute(context=context) 



        # Validar depois https://stackoverflow.com/questions/70196768/airflow-bigquery-to-gcs-operator-multiple-output-destination