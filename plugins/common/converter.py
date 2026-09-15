import pandas as pd
from pathlib import Path

class Converter:

    def __init__(self, TypeOrig,TypeDest,PathOrig,FileOrig):
        self.TypeOrig = TypeOrig
        self.TypeDest = TypeDest
        self.PathOrig = PathOrig
        self.FileOrig = FileOrig

    def convert(self):
        
        FileTypeOrigem = self.TypeOrig
        FileTypeDestination = self.TypeDest
        PathOrigin = self.PathOrig
        FileOrigin = self.FileOrig
        DestinationPath = self.PathOrig
        DestinationFile = Path(FileOrigin).stem


        print(f'Converter o arquivo {FileOrigin} de {FileTypeOrigem} para {FileTypeDestination}')
        
        if (FileTypeOrigem == 'csv'):
            if(FileTypeDestination == 'parquet'):
                df = pd.read_csv(f'{PathOrigin}/{FileOrigin}')
                df.to_parquet(f'{DestinationPath}/{DestinationFile}.{FileTypeDestination}')
            elif (FileTypeDestination == 'orc'):
                df = pd.read_csv(f'{PathOrigin}/{FileOrigin}',dtype=str)
                df.to_orc(f'{DestinationPath}/{DestinationFile}.{FileTypeDestination}')
        elif (FileTypeOrigem == 'parquet'):
            if(FileTypeDestination == 'csv'):
                df = pd.read_parquet(f'{PathOrigin}/{FileOrigin}.{FileTypeOrigem}')
                df.to_csv(f'{DestinationPath}/{DestinationFile}.{FileTypeDestination}',header=True,index=False)
            elif (FileTypeDestination == 'orc'):
                df = pd.read_parquet(f'{PathOrigin}/{FileOrigin}.{FileTypeOrigem}')
                df.to_orc(f'{DestinationPath}/{DestinationFile}.{FileTypeDestination}')

