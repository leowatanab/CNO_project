import os
import io
import zipfile
import tempfile
import requests
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm
import shutil
import duckdb
import re
import time
import pandas as pd
from datetime import datetime


# Global settings
CNO_URL = "https://arquivos.receitafederal.gov.br/dados/cno/cno.zip"

SLEEP_SECONDS = 0.6
MAX_TENTATIVAS = 5
BATCH_SIZE = 100
LOG_INTERVAL = 100
RETRY_DAYS = 7

TABLE_CNPJ = "cnpj_cadastral"
TABLE_ORIGEM_CNPJ = "cno_vinculos"


# Connect to MotherDuck database
def get_md_connection():
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise ValueError("❌ MOTHERDUCK_TOKEN não encontrado")
    return duckdb.connect(f"md:?motherduck_token={token}")


# Utilities functions
def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def padronizar_cnpj(cnpj):
    return re.sub(r"\D", "", str(cnpj)).zfill(14)


def clean_digits(v):
    return re.sub(r"\D", "", v) if v else None


# Download with retries and progress bar
def download_with_progress(url: str, dest_path: str):
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    total = None
    try:
        head = session.head(url, timeout=30)
        if "Content-Length" in head.headers:
            total = int(head.headers["Content-Length"])
    except Exception:
        pass

    with session.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f, tqdm(
            total=total, unit="B", unit_scale=True, desc="Baixando CNO"
        ) as pbar:
            for chunk in r.iter_content(chunk_size=4 * 1024 * 1024):
                if chunk:
                    f.write(chunk)
                    if total:
                        pbar.update(len(chunk))


# Extract and load CNO data
def extract_and_load_cno():
    workdir = tempfile.mkdtemp(prefix="cno_")
    zip_path = os.path.join(workdir, "cno.zip")
    utf8_dir = os.path.join(workdir, "utf8")
    os.makedirs(utf8_dir, exist_ok=True)

    try:
        download_with_progress(CNO_URL, zip_path)

        with zipfile.ZipFile(zip_path) as z:
            csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]

            con = get_md_connection()
            con.execute("CREATE DATABASE IF NOT EXISTS cno")
            con.execute("USE cno")
            con.execute("PRAGMA threads = 8")

            for file in tqdm(csv_files, desc="Importando CSVs"):
                table_name = (
                    file.split("/")[-1]
                        .replace(".csv", "")
                        .replace("-", "_")
                        .lower()
                )

                with z.open(file) as fpeek:
                    head = fpeek.read(100_000)

                encoding = "utf-8"
                try:
                    head.decode("utf-8")
                except UnicodeDecodeError:
                    encoding = "latin-1"

                out_path = os.path.join(utf8_dir, os.path.basename(file))
                if encoding == "utf-8":
                    with z.open(file) as src, open(out_path, "wb") as dst:
                        shutil.copyfileobj(src, dst)
                else:
                    with z.open(file) as src, \
                         io.TextIOWrapper(src, encoding="latin-1", errors="replace") as txt, \
                         open(out_path, "w", encoding="utf-8") as dst:
                        shutil.copyfileobj(txt, dst)

                staging = f"{table_name}__staging"
                con.execute(f"DROP TABLE IF EXISTS {qi(staging)}")
                con.execute(f"""
                    CREATE TEMP TABLE {qi(staging)} AS
                    SELECT *
                    FROM read_csv_auto(?, HEADER=TRUE, ALL_VARCHAR=TRUE);
                """, [out_path])

                if not con.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name=?",
                    [table_name]
                ).fetchone():
                    con.execute(f"CREATE TABLE {qi(table_name)} AS SELECT * FROM {qi(staging)}")
                else:
                    con.execute(f"""
                        INSERT INTO {qi(table_name)}
                        SELECT * FROM {qi(staging)}
                        EXCEPT
                        SELECT * FROM {qi(table_name)}
                    """)

            con.close()
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


# Transform data, creating cno_base table
def transform_data():
    con = get_md_connection()
    con.execute("USE cno")

    con.execute("""
        CREATE OR REPLACE TABLE cno_base AS
        SELECT
            c.*,
            a.* EXCLUDE (CNO),
            v.* EXCLUDE (CNO)
        FROM cno c
        LEFT JOIN cno_areas a USING (CNO)
        LEFT JOIN cno_vinculos v USING (CNO)
    """)

    con.close()


# Search and enrich CNPJs with BrasilAPI
session = requests.Session()
session.headers.update({"User-Agent": "cnpj-enrichment/1.0"})


def get_cnpj_info(cnpj):
    for i in range(MAX_TENTATIVAS):
        try:
            r = session.get(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}", timeout=10)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                return {"_status": "cnpj_nao_encontrado"}
            if r.status_code == 429:
                time.sleep(10)
        except Exception:
            return {"_status": "erro_api"}
    return {"_status": "erro_api"}

# Create CNPJ table
def criar_tabela_cnpj(con):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_CNPJ} (
            cnpj VARCHAR,
            razao_social VARCHAR,
            nome_fantasia VARCHAR,
            municipio VARCHAR,
            uf VARCHAR,
            situacao_cadastral VARCHAR,
            porte VARCHAR,
            natureza_juridica VARCHAR,
            data_abertura DATE,
            data_consulta TIMESTAMP,
            status_consulta VARCHAR
        )
    """)
    con.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_cnpj
        ON {TABLE_CNPJ}(cnpj)
    """)


def enrich_cnpjs():
    con = get_md_connection()
    con.execute("USE cno")
    criar_tabela_cnpj(con)

    df = con.execute(f"""
        SELECT DISTINCT
            regexp_replace("NI do responsável", '\\D', '', 'g') AS cnpj
        FROM {TABLE_ORIGEM_CNPJ}
        WHERE "NI do responsável" IS NOT NULL
    """).df()

    buffer = []

    for i, cnpj in enumerate(df["cnpj"], start=1):
        info = get_cnpj_info(padronizar_cnpj(cnpj))

        row = {
            "cnpj": padronizar_cnpj(cnpj),
            "data_consulta": datetime.utcnow(),
            "status_consulta": info.get("_status", "ok")
        }

        if "_status" not in info:
            row.update({
                "razao_social": info.get("razao_social"),
                "nome_fantasia": info.get("nome_fantasia"),
                "municipio": info.get("municipio"),
                "uf": info.get("uf"),
                "situacao_cadastral": info.get("descricao_situacao_cadastral"),
                "porte": info.get("porte"),
                "natureza_juridica": info.get("natureza_juridica"),
                "data_abertura": (
                    datetime.strptime(info["data_inicio_atividade"], "%Y-%m-%d").date()
                    if info.get("data_inicio_atividade") else None
                )
            })

        buffer.append(row)

        if len(buffer) >= BATCH_SIZE:
            con.register("df_tmp", pd.DataFrame(buffer))
            con.execute(f"INSERT OR IGNORE INTO {TABLE_CNPJ} SELECT * FROM df_tmp")
            buffer.clear()

        if i % LOG_INTERVAL == 0:
            print(f"⏳ {i} CNPJs processados")

        time.sleep(SLEEP_SECONDS)

    if buffer:
        con.register("df_tmp", pd.DataFrame(buffer))
        con.execute(f"INSERT OR IGNORE INTO {TABLE_CNPJ} SELECT * FROM df_tmp")

    con.close()


# Main pipeline dataflow
def main():
    print("🚀 PIPELINE INICIADO")
    extract_and_load_cno()
    transform_data()
    enrich_cnpjs()
    print("🎉 PIPELINE FINALIZADO COM SUCESSO")


if __name__ == "__main__":
    main()
