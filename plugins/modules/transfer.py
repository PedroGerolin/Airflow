
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator # type: ignore
import os

class TransferFile:

    def __init__(self, PathOrig,PathDest):
        self.PathOrig = PathOrig
        self.FileOrig = PathDest


    def transfer_file_gcs(self):
            data_folder = self.PathOrig
            bucket_name = 'gerolin_etl'
            gcs_conn_id = 'google_cloud_default'

            orc_files = [file for file in os.listdir(data_folder) if file.endswith('dados_brutos.orc')]

            for file in orc_files:
                local_file_path = os.path.join(data_folder, file)
                gcs_file_path = self.FileOrig + file

                print(f"Arquivo será usada:{local_file_path}")

                upload_to_gcs = LocalFilesystemToGCSOperator(
                    task_id='upload_to_gcs',
                    src=local_file_path,
                    dst=gcs_file_path,
                    bucket=bucket_name,
                    gcp_conn_id=gcs_conn_id
                )
                upload_to_gcs.execute(context=None)