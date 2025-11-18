from .proc_log import ProcLog

import json
import os
import pandas as pd

from SQLServer import SQLServer
from SQLLoader import SQLLoader

class SQLItemFaturaSimplificado(ProcLog):
    
    def proc_item_fatura_simplificado(self, path_run, trat=None):
        self.__get_path(path_run)
        self.__file_list()
        
        for f in self.files[:]:
            self.__el(f, trat)
        
    def __get_path(self, path_run):
        with open(os.path.join(path_run, 'path.json'), 'r') as f:
            path_data = json.loads(f.read())
            self.path = os.path.join(path_data['path'].format(login=os.getlogin()), 'item_fatura')
        
    def __file_list(self):
        self.files = [f for f in os.listdir(self.path) if f.endswith('.csv.zip') and f.startswith('ITEM_FATURA_SIMPLIFICADO')]
        
    def __get_query_insert(self):
        return self._ProcLog__exec(
            title = None
            , module = 'item fatura simplificado'
            , crud = 'insert'
            , name = 'insert car'
        )
    
    def __get_query_trucate(self):
        return self._ProcLog__exec(
            title = None
            , module = 'item fatura simplificado'
            , crud = 'truncate'
            , name = 'truncate car'
        )
    
    def __get_query_execute(self):
        return self._ProcLog__exec(
            title = None
            , module = 'item fatura simplificado'
            , crud = 'execute'
            , name = 'execute carga'
        )
    
    def __el(self, file, trat):
        self.__read_csv(file)
        self.__load_data(trat)
        self.__execute_procedure()
    
    def __read_csv(self, file):
        self.df = pd.read_csv(
            os.path.join(self.path, file)
            , sep=';'
            , encoding='iso-8859-1'
            , date_format='%d/%m/%Y'
            , parse_dates=['dat_corte_fatura']
            , decimal=','
            , thousands='.'
            , compression='zip'
            , chunksize=50_000
    #         , on_bad_lines='warn'
        )
        print('Read csv to Dataframe')
        
    def __load_data(self, trat):
        sqlLoader = SQLLoader(
            self.df
            , chunk=True
            , query=self.__get_query_insert()
            , query_truncate=self.__get_query_trucate()
            , dns='SNEPDB33V'
            , database_name='mktvas'
            , dates_field=['num_ano_mes_referencia', 'dat_corte_fatura']
            , func=trat
        )
        print('Load data into CAR table')
        
    def __execute_procedure(self):      
        sql = SQLServer(
            dns='SNEPDB33V'
            , database_name='mktvas'
        )
        sql.execute_query(
            query=self.__get_query_execute()
            , commit=True
        )
        print('Consolidate data into CONS table')
