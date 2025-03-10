import csv
import os
import chardet
import pandas as pd
import unicodedata

class FileTransformer:

    def __init__(self, file_path, file_name, new_file_name=None):
        self.file_path = file_path
        self.file_name = file_name
        self.new_file_name = new_file_name if new_file_name else file_name
        self.full_path = f'{self.file_path}{self.file_name}'

    def detect_encoding(self, full_path=None):
        with open(full_path if full_path else self.full_path, 'rb') as f:
            result = chardet.detect(f.read())
        return result['encoding']
    
    def header_normalize(self, delete_original_file=False):
        if not os.path.isfile(self.full_path):
            print(f'Arquivo {self.full_path} não encontrado')
            return
        
        df = pd.read_csv(self.full_path
                        ,dtype=str
                        ,sep=';'
                        ,quotechar='"'
                        ,header=0
                        ,encoding=self.detect_encoding()
                        )
        
        new_header = []
        for col in df.columns:
            col = col.replace(' ','')
            col = col.replace('-','_')
            col = unicodedata.normalize('NFKD', col)
            col = col.encode('ascii', 'ignore')
            col = col.decode("utf-8")
            print(col)
            new_header.append(col)
        df.columns = new_header

        df.to_csv(f'{self.file_path}{self.new_file_name}'
                    ,sep='|'
                    ,quotechar='"'
                    ,quoting=csv.QUOTE_ALL
                    ,index=False
                    ,encoding='UTF-8'
                )
        if delete_original_file:
            os.remove(self.full_path)
    
    def split_file(self, file, group_by, format, delete_original_file=False):
        if not os.path.isfile(f'{self.file_path}{file}'):
            print(f'Arquivo {self.file_path}{file} não encontrado')
            return
        
        df = pd.read_csv(f'{self.file_path}{file}'
                        ,dtype=str
                        ,sep='|'
                        ,quotechar='"'
                        ,header=0
                        ,encoding=self.detect_encoding(f'{self.file_path}{file}')
                        )
        df.groupby(by=pd.to_datetime(df[group_by],format=format).dt.date).apply(lambda x: x.to_csv(f'{self.file_path}{self.new_file_name.replace('.csv','')}_{pd.to_datetime(x.name).date()}.csv'
                                                        ,sep='|'
                                                        ,quotechar='"'
                                                        ,quoting=csv.QUOTE_ALL
                                                        ,index=False
                                                        ,encoding='UTF-8'
                                                        )
                                    )
        if delete_original_file:
            os.remove(f'{self.file_path}{file}')