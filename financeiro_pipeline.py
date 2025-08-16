from airflow import DAG
from airflow.decorators import task
import pendulum
import pandas as pd
import os

# Caminhos locais
INPUT_FOLDER = "/home/luizfp22/projects/financeiro/input"
OUTPUT_FILE = "/home/luizfp22/projects/financeiro/output/controle_financeiro.csv"

# Extensões permitidas
ALLOWED_EXTS = [".csv", ".xlsx", ".xls"]

def _latest_file(folder: str, extensions: list[str] | None = None) -> str:
    files = [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if os.path.isfile(os.path.join(folder, f))
    ]
    if extensions:
        files = [f for f in files if f.lower().endswith(tuple(extensions))]
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo permitido encontrado em: {folder}")
    return max(files, key=os.path.getmtime)


with DAG(
    dag_id="financeiro_pipeline",
    start_date=pendulum.datetime(2024, 8, 15, tz="UTC"),
    schedule=None,  # você pode agendar se quiser
    catchup=False,
    tags=["financeiro", "pessoal"],
):

    @task
    def extract_latest_file() -> str:
        """Localiza o arquivo mais recente na pasta de input e retorna o caminho completo."""
        latest = _latest_file(INPUT_FOLDER, ALLOWED_EXTS)
        return latest

    @task
    def load_to_dataframe(src_path: str) -> str:
        """
        Lê o arquivo no pandas e salva o DataFrame como controle_financeiro.csv
        """
        # cria pasta de saída se não existir
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        filename = os.path.basename(src_path)
        if src_path.endswith(".csv"):
            df = pd.read_csv(src_path)
            if filename.upper().startswith("PC_FINANCIAL"):
                df = df.drop(columns=["Card Holder Name", "Time"], errors="ignore")
                df['Card'] = "PC Financial"
        else:
            raise ValueError(f"Formato não suportado: {src_path}")

        # salva sempre como CSV
        df.to_csv(OUTPUT_FILE, index=False)
        return OUTPUT_FILE

    output_path = load_to_dataframe(extract_latest_file())
