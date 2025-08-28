import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime
import logging
import unicodedata

INPUT_DIR = '/home/luizfp22/projects/financeiro/input'
EXCLUDE_DESCRIPTIONS = [
        'Direct deposit from PINHEIROS DE AR',
        'Direct deposit from Luiz Fernando P',
        'GIC cancelled from 123355048',
        'GIC purchase to 123355048',
        'Interac e-Transfer sent to WealthSimple',
        'Interac e-Transfer sent to Luiz PC Financial',
        'Transfer from 114728209 to 111195544',
        'Interac e-Transfer sent to Gey',
        'Interac e-Transfer sent to CIBC',
        'Interac e-Transfer received from LUIZ F PINHEIROS DE ARAUJO',
        'Interac received from LUIZ F PINHEIROS DE ARAUJO',
        'Interac Transfer received from LUIZ F PINHEIROS DE ARAUJO',
        'Interac e-Transfer received from Luiz Fernando Pinheiros de Araujo',
        'Interac e-Transfer sent to Simplii Nando',
        'Interac e-Transfer received from geyseearaujo@hotmail.com to bk.eq.lf@proton.me',
        'Transfer from geyseearaujo@hotmail.com to bk.eq.lf@proton.me',
        'interac e-transfer received from  luiz f pinheiros de araujo',
        'interac e-transfer received from  luiz fernando pinheiros de araujo',
        'Interac e-Transfer received from  LUIZ F PINHEIROS DE ARAUJO',
        'Interac e-Transfer received from  Luiz Fernando Pinheiros de Araujo',
        'Payment to CIBC VISA',
        'Payment to ROGERS BANK MASTERCARD',
        'payment to  cibc visa',
        'payment to  mastercard pc financial',
        'Payment to  MASTERCARD PC FINANCIAL',
        'Payment to  ROGERS BANK MASTERCARD',
        'Payment to  CIBC VISA',
        'Payment to  CIBC MASTERCARD',
        'Payment to CIBC MASTERCARD',
        'Payment to PC Financial',
        'Payment to MASTERD PC FINANCIAL',
        'Payment to Mastercard PC Financial',
        'Payment to MR ERARD PC FINANCIAL',
        'Transfer from 200225325 to 114728209',
        'Transfer from 114728209 to 200225325',
        'Transfer from 115853228 to 114728209',
        'Transfer from 111195544 to 114728209',
        'Transfer from  111195544  to  114728209'
]

@dag(
    dag_id='process_eq_files',
    start_date=datetime(2025, 8, 25),
    schedule=None,  # Run manually or set a cron schedule as needed
    catchup=False,
    tags=['pessoal','financeiro'],
    default_args={'retries': 3}
)
def process_eq_dag():
    
    @task
    def process_nando():
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        try:
            df = pd.read_csv(f'{INPUT_DIR}/EQ_Nando.csv')
            df['Amount'] = df['Amount'].replace('[^0-9.-]', '', regex=True)
            df['Amount'] = df['Amount'].astype(float)
            # Filtrar linhas por comparação exata, sem normalização
            df_filtered = df[~df['Description'].isin(EXCLUDE_DESCRIPTIONS)]
            # Remover coluna Balance se existir
            cols_to_drop = [col for col in ['Balance'] if col in df_filtered.columns]
            df_final = df_filtered.drop(columns=cols_to_drop)
            # Salvar temporário
            temp_file = f'{INPUT_DIR}/EQ_Nando_processed.csv'
            df_final.to_csv(temp_file, index=False)
            logger.info(f"Arquivo processado: {temp_file}")
            return temp_file
        except Exception as e:
            logger.error(f"Erro ao processar EQ_Nando.csv: {e}")
            raise

    @task
    def process_future():
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        try:
            df = pd.read_csv(f'{INPUT_DIR}/EQ_Future.csv')
            df['Amount'] = df['Amount'].replace('[^0-9.-]', '', regex=True)
            df['Amount'] = df['Amount'].astype(float)
            # Filtrar linhas por comparação exata, sem normalização
            df_filtered = df[~df['Description'].isin(EXCLUDE_DESCRIPTIONS)]
            # Remover coluna Balance se existir
            cols_to_drop = [col for col in ['Balance'] if col in df_filtered.columns]
            df_final = df_filtered.drop(columns=cols_to_drop)
            # Salvar temporário
            temp_file = f'{INPUT_DIR}/EQ_Future_processed.csv'
            df_final.to_csv(temp_file, index=False)
            logger.info(f"Arquivo processado: {temp_file}")
            return temp_file
        except Exception as e:
            logger.error(f"Erro ao processar EQ_Future.csv: {e}")
            raise

    @task
    def combine_files(nando_file: str, future_file: str):
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        try:
            df_nando = pd.read_csv(nando_file)
            df_future = pd.read_csv(future_file)
            df_combined = pd.concat([df_nando, df_future], ignore_index=True)
            df_combined.to_csv(f'{INPUT_DIR}/EQ_Final.csv', index=False)
            logger.info(f"Arquivo final salvo em {INPUT_DIR}/EQ_Final.csv")
        except Exception as e:
            logger.error(f"Erro ao concatenar arquivos: {e}")
            raise

    # Definir dependências
    nando_task = process_nando()
    future_task = process_future()
    combine_files(nando_task, future_task)

process_eq_dag()