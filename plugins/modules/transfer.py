
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import os

class TransferFile:

    def __init__(self, PathOrig,PathDest,FileSuffix=None, FilePrefix=None, DynamicPath=False):
        self.PathOrig = PathOrig
        self.PathDest = PathDest
        self.FileSuffix = FileSuffix
        self.FilePrefix = FilePrefix
        self.DynamicPath = DynamicPath

    def transfer_file_gcs(self, delete_original_file=False):
            data_folder = self.PathOrig
            bucket_name = 'gerolin_etl'
            gcs_conn_id = 'google_cloud_default'
            
            if self.FileSuffix:
                files = [file for file in os.listdir(data_folder) if file.endswith(self.FileSuffix)]
            elif self.FilePrefix:
                files = [file for file in os.listdir(data_folder) if file.startswith(self.FilePrefix)]

            if not files:
                    print(f'Arquivo não encontrado')
                    return
            
            for file in files:
                local_file_path = os.path.join(data_folder, file)

                if self.DynamicPath:
                    gcs_file_path = self.PathDest + file.replace(self.FilePrefix,'').replace('.csv','') + '/'
                elif self.FileSuffix:
                    gcs_file_path = self.PathDest + file

                upload_to_gcs = LocalFilesystemToGCSOperator(
                    task_id='upload_to_gcs',
                    src=local_file_path,
                    dst=gcs_file_path,
                    bucket=bucket_name,
                    gcp_conn_id=gcs_conn_id
                )
                upload_to_gcs.execute(context=None)
                
                if delete_original_file:
                    os.remove(local_file_path)
            
            
                
                