import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime
import logging
import unicodedata
import re

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

def clean_amount_column(df):
    """
    Limpa a coluna Amount preservando corretamente os sinais + e -
    """
    def clean_amount_value(value):
        if pd.isna(value):
            return 0.0
        # Converte para string
        amount_str = str(value).strip()
        # Se já é um número válido, verifica se precisa de tratamento especial
        try:
            result = float(amount_str)
            # Se o resultado é menor que 10 e tem 3 casas decimais, pode ser separador de milhares
            if result < 10 and len(amount_str.split('.')[-1]) == 3:
                # Pode ser separador de milhares, trata como string
                pass
            # Se o resultado é muito grande (mais de 10000) e não tem ponto, pode precisar de separador decimal
            elif result > 10000 and '.' not in amount_str:
                # Pode precisar de separador decimal, trata como string
                pass
            else:
                return result
        except ValueError:
            pass
        # Remove caracteres indesejados mas preserva sinais e números
        # Primeiro, identifica se tem sinal negativo
        is_negative = '-' in amount_str
        # Verifica se está entre parênteses (formato comum para valores negativos)
        is_parentheses = amount_str.startswith('(') and amount_str.endswith(')')
        # Remove todos os caracteres exceto dígitos, ponto e vírgula
        cleaned = re.sub(r'[^\d.,]', '', amount_str)
        # Se não tem ponto nem vírgula, verifica se precisa adicionar separador decimal
        if '.' not in cleaned and ',' not in cleaned:
            # Se tem 6 dígitos ou mais, provavelmente precisa de separador decimal
            if len(cleaned) >= 6:
                # Adiciona ponto antes dos últimos 2 dígitos (formato comum para centavos)
                cleaned = cleaned[:-2] + '.' + cleaned[-2:]
        # Substitui vírgula por ponto se necessário (para casos de formato europeu)
        cleaned = cleaned.replace(',', '.')
        # Se tiver múltiplos pontos, trata como separador de milhares
        if cleaned.count('.') > 1:
            # Remove todos os pontos exceto o último (que é o decimal)
            parts = cleaned.split('.')
            cleaned = ''.join(parts[:-1]) + '.' + parts[-1]
        # Se tem apenas um ponto, verifica se é separador de milhares ou decimal
        elif cleaned.count('.') == 1:
            # Se o número antes do ponto tem mais de 3 dígitos, provavelmente é separador de milhares
            parts = cleaned.split('.')
            # Se tem 4 dígitos antes do ponto e 2 dígitos depois, pode ser separador decimal adicionado
            if len(parts[0]) == 4 and len(parts[1]) == 2:
                # Mantém como separador decimal
                pass
            elif len(parts[0]) > 3 and len(parts[1]) <= 2:
                # É separador de milhares, remove o ponto
                cleaned = parts[0] + parts[1]
            # Se tem 1 dígito antes do ponto e 3 dígitos depois, provavelmente é separador de milhares
            elif len(parts[0]) == 1 and len(parts[1]) == 3:
                # É separador de milhares, remove o ponto
                cleaned = parts[0] + parts[1]
        # Converte para float
        try:
            result = float(cleaned) if cleaned else 0.0
            # Aplica o sinal negativo se necessário
            return -result if is_negative or is_parentheses else result
        except ValueError:
            return 0.0
    df['Amount'] = df['Amount'].apply(clean_amount_value)
    return df

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
            # Limpa a coluna Amount preservando sinais
            df = clean_amount_column(df)   
            # Debug: mostra alguns valores para verificar
            logger.info("Primeiros 5 valores de Amount após limpeza:")
            logger.info(df[['Description', 'Amount']].head().to_string())
            # Filtrar linhas por comparação exata, sem normalização
            df_filtered = df[~df['Description'].isin(EXCLUDE_DESCRIPTIONS)]
            # Remover coluna Balance se existir
            cols_to_drop = [col for col in ['Balance'] if col in df_filtered.columns]
            df_final = df_filtered.drop(columns=cols_to_drop)
            # Salvar temporário
            temp_file = f'{INPUT_DIR}/EQ_Nando_processed.csv'
            df_final.to_csv(temp_file, index=False)
            logger.info(f"Arquivo processado: {temp_file}")
            logger.info(f"Valores negativos preservados: {(df_final['Amount'] < 0).sum()}")
            logger.info(f"Valores positivos preservados: {(df_final['Amount'] > 0).sum()}")
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
            # Limpa a coluna Amount preservando sinais
            df = clean_amount_column(df)
            # Debug: mostra alguns valores para verificar
            logger.info("Primeiros 5 valores de Amount após limpeza:")
            logger.info(df[['Description', 'Amount']].head().to_string())
            # Filtrar linhas por comparação exata, sem normalização
            df_filtered = df[~df['Description'].isin(EXCLUDE_DESCRIPTIONS)]
            # Remover coluna Balance se existir
            cols_to_drop = [col for col in ['Balance'] if col in df_filtered.columns]
            df_final = df_filtered.drop(columns=cols_to_drop)
            # Salvar temporário
            temp_file = f'{INPUT_DIR}/EQ_Future_processed.csv'
            df_final.to_csv(temp_file, index=False)
            logger.info(f"Arquivo processado: {temp_file}")
            logger.info(f"Valores negativos preservados: {(df_final['Amount'] < 0).sum()}")
            logger.info(f"Valores positivos preservados: {(df_final['Amount'] > 0).sum()}")
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
            
            # Garantir que a coluna Amount seja numérica
            df_nando['Amount'] = pd.to_numeric(df_nando['Amount'], errors='coerce')
            df_future['Amount'] = pd.to_numeric(df_future['Amount'], errors='coerce')
            
            df_combined = pd.concat([df_nando, df_future], ignore_index=True)
            # Debug final
            logger.info("Estatísticas finais do arquivo combinado:")
            logger.info(f"Total de registros: {len(df_combined)}")
            logger.info(f"Valores negativos: {(df_combined['Amount'] < 0).sum()}")
            logger.info(f"Valores positivos: {(df_combined['Amount'] > 0).sum()}")
            logger.info(f"Valores zero: {(df_combined['Amount'] == 0).sum()}")
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