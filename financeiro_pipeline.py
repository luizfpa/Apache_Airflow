from airflow import DAG
from airflow.decorators import task
import pendulum
import pandas as pd
import logging
import os

# Caminhos locais
INPUT_FOLDER = "/home/luizfp22/projects/financeiro/input"
OUTPUT_FILE = "/home/luizfp22/projects/financeiro/output/controle_financeiro.csv"

# Extensões permitidas
ALLOWED_EXTS = [".csv", ".xlsx", ".xls"]

def _latest_file_by_prefix(folder: str, prefix: str, extensions: list[str] | None = None) -> str | None:
    files = [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if os.path.isfile(os.path.join(folder, f)) and f.upper().startswith(prefix.upper())
    ]
    if extensions:
        files = [f for f in files if f.lower().endswith(tuple(extensions))]
    if not files:
        return None
    return max(files, key=os.path.getmtime)

with DAG(
    dag_id="financeiro_pipeline",
    start_date=pendulum.datetime(2024, 8, 15, tz="UTC"),
    schedule=None,  
    catchup=False,
    tags=["financeiro", "pessoal"],
):

    @task
    def find_pc_file() -> str | None:
        """Localiza o arquivo mais recente começando com PC_ na pasta de input."""
        return _latest_file_by_prefix(INPUT_FOLDER, "PC_", ALLOWED_EXTS)

    @task
    def find_ws_file() -> str | None:
        """Localiza o arquivo mais recente começando com WS_ na pasta de input."""
        return _latest_file_by_prefix(INPUT_FOLDER, "WS_", ALLOWED_EXTS)

    @task
    def find_cibcn_file() -> str | None:
        """Localiza o arquivo mais recente começando com cibc_nando na pasta de input."""
        return _latest_file_by_prefix(INPUT_FOLDER, "cibc_nando", ALLOWED_EXTS)
    
    @task
    def process_pc_file(src_path: str | None) -> pd.DataFrame | None:
        if not src_path:
            return None
        if not src_path.endswith(".csv"):
            raise ValueError(f"Formato não suportado para PC_: {src_path}")
        df = pd.read_csv(src_path)
        df = df.drop(columns=["Card Holder Name", "Time"], errors="ignore")
        df['Card'] = "PC Financial"
        if 'Type' in df.columns:
            df = df[df['Type'] != 'PAYMENT']
        return df[['Date', 'Description', 'Amount', 'Type', 'Card']] if all(col in df.columns for col in ['Date', 'Description', 'Amount', 'Type']) else None

    @task
    def process_ws_file(src_path: str | None) -> pd.DataFrame | None:
        if not src_path:
            return None
        if not src_path.endswith(".csv"):
            raise ValueError(f"Formato não suportado para WS_: {src_path}")
        df = pd.read_csv(src_path)
        df = df.drop(columns=["Status"], errors="ignore")
        df['Card'] = "Wealthsimple"
        return df[['Date', 'Description', 'Amount', 'Type', 'Card']] if all(col in df.columns for col in ['Date', 'Description', 'Amount', 'Type']) else None

    @task
    def process_cibcn_file(src_path: str | None) -> pd.DataFrame | None:
        """Processa arquivo cibc_nando, ajusta Amount para negativo, remove linhas sem Amount, deleta colunas desnecessárias e padroniza."""
        if not src_path:
            return None
        if not src_path.endswith(".csv"):
            raise ValueError(f"Formato não suportado para cibc_nando: {src_path}")
        df = pd.read_csv(src_path)
        # Deleta colunas Payment e Card Number
        df = df.drop(columns=["Payment", "Card Number"], errors="ignore")
        # Remove linhas sem valor em Amount
        if 'Amount' in df.columns:
            df = df[df['Amount'].notna() & (df['Amount'] != '')]
            # Torna Amount negativo
            df['Amount'] = df['Amount'] * -1
        # Adiciona colunas Type e Card
        df['Type'] = "Purchase"
        df['Card'] = "CIBC Nando"
        # Verifica e retorna colunas padrão
        return df[['Date', 'Description', 'Amount', 'Type', 'Card']] if all(col in df.columns for col in ['Date', 'Description', 'Amount']) else None

    @task
    def merge_and_append(pc_df: pd.DataFrame | None, ws_df: pd.DataFrame | None, cibcn_df: pd.DataFrame | None) -> str:
        """Mescla DataFrames, remove duplicatas e adiciona ao arquivo de saída."""
        # Configura logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        dfs = [df for df in [pc_df, ws_df, cibcn_df] if df is not None]
        if not dfs:
            raise ValueError("Nenhum arquivo PC_, WS_ ou cibc_nando encontrado para processar.")
        merged_df = pd.concat(dfs, ignore_index=True)
        
        # Remove duplicatas no merged_df baseado em todas as colunas
        merged_df = merged_df.drop_duplicates(subset=['Date', 'Description', 'Amount', 'Type', 'Card'], keep='last')
        
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        if os.path.exists(OUTPUT_FILE):
            existing_df = pd.read_csv(OUTPUT_FILE)
            # Combina com dados existentes e remove duplicatas novamente
            final_df = pd.concat([existing_df, merged_df], ignore_index=True)
            final_df = final_df.drop_duplicates(subset=['Date', 'Description', 'Amount', 'Type', 'Card'], keep='last')
        else:
            final_df = merged_df
        
        # Loga número de linhas após deduplicação
        logger.info(f"Total de linhas após deduplicação: {len(final_df)}")
        final_df.to_csv(OUTPUT_FILE, index=False)
        return OUTPUT_FILE
    
    @task
    def clean_output_file(output_path: str) -> str:
        """Limpa a coluna Amount, remove transações sem data, ordena por Date e salva."""
        df = pd.read_csv(output_path)
        
        # Configura logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        # Remove transações com Date ausente ou vazio
        if 'Date' in df.columns:
            initial_rows = len(df)
            df = df[df['Date'].notna() & (df['Date'].str.strip() != '')]
            removed_rows = initial_rows - len(df)
            if removed_rows > 0:
                logger.info(f"Removidas {removed_rows} transações sem data válida.")
        
        # Limpa Amount: remove aspas e mantém como string
        if 'Amount' in df.columns:
            df['Amount'] = df['Amount'].astype(str).str.replace('"', '', regex=False).str.strip()
            # Loga valores que parecem inválidos (não numéricos, exceto sinal)
            non_numeric = df[df['Amount'].str.match(r'^-?\d*\.?\d*$', na=False) == False]
            if not non_numeric.empty:
                logger.warning(f"Valores não numéricos em Amount: {non_numeric[['Amount']].to_dict()}")

        # Deixa a coluna Type com apenas a primeira letra maiúscula
        if 'Type' in df.columns:
            df['Type'] = df['Type'].astype(str).str.capitalize()

        # Ordena por Date (tratado como string, assumindo formato consistente MM/DD/YYYY)
        if 'Date' in df.columns:
            df = df.sort_values(by='Date', ascending=True, na_position='last')
        
        df.to_csv(output_path, index=False)
        return output_path

    pc_path = find_pc_file()
    ws_path = find_ws_file()
    cibcn_path = find_cibcn_file()
    pc_processed = process_pc_file(pc_path)
    ws_processed = process_ws_file(ws_path)
    cibcn_processed = process_cibcn_file(cibcn_path)
    merged_path = merge_and_append(pc_processed, ws_processed, cibcn_processed)
    cleaned_path = clean_output_file(merged_path)