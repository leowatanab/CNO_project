import os
import io
import zipfile
import tempfile
import shutil
import re
import time
import requests
import duckdb
import pandas as pd
from datetime import datetime
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm

# =====================================================
# CONFIGURAÇÕES
# =====================================================
CNO_URL = "https://arquivos.receitafederal.gov.br/index.php/s/PC6732BXG9B98W3/download?path=%2F&files=cno.zip"

TABLE_DESTINO = "cnpj_cadastral"

SLEEP_SECONDS = 0.6
MAX_TENTATIVAS = 5
LOG_INTERVAL = 100
BATCH_SIZE = 200

# =====================================================
# CONEXÃO MOTHERDUCK
# =====================================================
def get_md_connection():
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise ValueError("❌ MOTHERDUCK_TOKEN não encontrado")

    con = duckdb.connect(f"md:cno?motherduck_token={token}")
    con.execute("USE main")
    return con

def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

# =====================================================
# EXTRAÇÃO + CARGA INCREMENTAL DO CNO
# =====================================================
def extract_and_load_raw(threads: int = 8):
    workdir = tempfile.mkdtemp(prefix="cno_")
    zip_path = os.path.join(workdir, "cno.zip")

    try:
        print("⬇️ Baixando CNO...")
        r = requests.get(CNO_URL, stream=True, timeout=120)
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(4 * 1024 * 1024):
                f.write(chunk)

        con = get_md_connection()
        con.execute("CREATE DATABASE IF NOT EXISTS cno")
        con.execute("USE cno")
        con.execute(f"PRAGMA threads={threads}")

        with zipfile.ZipFile(zip_path) as z:
            for file in z.namelist():
                if not file.lower().endswith(".csv"):
                    continue

                table = (
                    os.path.basename(file)
                    .replace(".csv", "")
                    .replace("-", "_")
                    .lower()
                )

                print(f"📄 {file} → {table}")

                with z.open(file) as src:
                    data = src.read()

                try:
                    data.decode("utf-8")
                    encoding = "utf-8"
                except UnicodeDecodeError:
                    encoding = "latin-1"

                csv_path = os.path.join(workdir, file)
                os.makedirs(os.path.dirname(csv_path), exist_ok=True)

                if encoding == "utf-8":
                    with open(csv_path, "wb") as f:
                        f.write(data)
                else:
                    with open(csv_path, "w", encoding="utf-8") as f:
                        f.write(data.decode("latin-1", errors="replace"))

                con.execute(f"""
                    CREATE TABLE IF NOT EXISTS {qi(table)} AS
                    SELECT *
                    FROM read_csv_auto('{csv_path}', ALL_VARCHAR=TRUE)
                """)

        con.close()
        print("✅ CNO carregado")

    finally:
        shutil.rmtree(workdir, ignore_errors=True)

# =====================================================
# TRANSFORMAÇÃO
# =====================================================
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
        LEFT JOIN (
            SELECT *
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY CNO ORDER BY rowid DESC) rn
                FROM cno_areas
            ) WHERE rn = 1
        ) a USING (CNO)
        LEFT JOIN (
            SELECT *
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY CNO ORDER BY rowid DESC) rn
                FROM cno_vinculos
            ) WHERE rn = 1
        ) v USING (CNO)
    """)

    total = con.execute("SELECT COUNT(*) FROM cno_base").fetchone()[0]
    print(f"✅ cno_base criada ({total:,})")

    con.close()

# =====================================================
# CNPJ
# =====================================================
def padronizar_cnpj(cnpj):
    return re.sub(r"\D", "", str(cnpj)).zfill(14)

def clean_digits(v):
    return re.sub(r"\D", "", v) if v else None

def get_cnpj_info(cnpj):
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            r = requests.get(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}", timeout=10)

            if r.status_code == 200:
                return r.json()

            if r.status_code == 404:
                return {"_status": "nao_encontrado"}

            time.sleep(tentativa * 2)

        except requests.exceptions.RequestException:
            time.sleep(tentativa * 2)

    return {"_status": "erro"}

# =====================================================
# TABELA DESTINO (CORRIGIDA)
# =====================================================
def criar_tabela_destino(con):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_DESTINO} (
            cnpj VARCHAR PRIMARY KEY,
            razao_social VARCHAR,
            nome_fantasia VARCHAR,
            logradouro VARCHAR,
            numero VARCHAR,
            bairro VARCHAR,
            municipio VARCHAR,
            uf VARCHAR,
            cep VARCHAR,
            situacao_cadastral VARCHAR,
            tipo_estabelecimento VARCHAR,
            porte VARCHAR,
            natureza_juridica VARCHAR,
            email VARCHAR,
            telefone_1 VARCHAR,
            telefone_2 VARCHAR,
            data_abertura DATE,
            data_consulta TIMESTAMP,
            status_consulta VARCHAR
        )
    """)
    print("✅ Tabela destino pronta")

# =====================================================
# PROCESSAMENTO
# =====================================================
def processar_cnpj(cnpj):
    info = get_cnpj_info(cnpj)

    if "_status" in info:
        return {
            "cnpj": cnpj,
            "data_consulta": datetime.utcnow(),
            "status_consulta": info["_status"]
        }

    return {
        "cnpj": cnpj,
        "razao_social": info.get("razao_social"),
        "nome_fantasia": info.get("nome_fantasia"),
        "logradouro": info.get("logradouro"),
        "numero": info.get("numero"),
        "bairro": info.get("bairro"),
        "municipio": info.get("municipio"),
        "uf": info.get("uf"),
        "cep": clean_digits(info.get("cep")),
        "situacao_cadastral": info.get("descricao_situacao_cadastral"),
        "tipo_estabelecimento": info.get("descricao_identificador_matriz_filial"),
        "porte": info.get("porte"),
        "natureza_juridica": info.get("natureza_juridica"),
        "email": info.get("email"),
        "telefone_1": clean_digits(info.get("ddd_telefone_1")),
        "telefone_2": clean_digits(info.get("ddd_telefone_2")),
        "data_abertura": (
            datetime.strptime(info["data_inicio_atividade"], "%Y-%m-%d").date()
            if info.get("data_inicio_atividade") else None
        ),
        "data_consulta": datetime.utcnow(),
        "status_consulta": "ok"
    }

def dados_cnpj():
    con = get_md_connection()
    criar_tabela_destino(con)

    df = con.execute("""
        SELECT DISTINCT regexp_replace("NI do responsável", '\\D', '', 'g') cnpj
        FROM cno.cno_vinculos
        WHERE "NI do responsável" IS NOT NULL
          AND regexp_replace("NI do responsável", '\\D', '', 'g')
              NOT IN (SELECT cnpj FROM cnpj_cadastral)
    """).df()

    cnpjs = df["cnpj"].dropna().tolist()
    total = len(cnpjs)
    print(f"🔎 {total} CNPJs novos")

    buffer = []

    for i, cnpj in enumerate(cnpjs, 1):
        buffer.append(processar_cnpj(padronizar_cnpj(cnpj)))

        if len(buffer) >= BATCH_SIZE:
            df_tmp = pd.DataFrame(buffer)
            con.register("df_tmp", df_tmp)

            con.execute("""
                INSERT OR IGNORE INTO cnpj_cadastral (
                    cnpj, razao_social, nome_fantasia, logradouro, numero,
                    bairro, municipio, uf, cep, situacao_cadastral,
                    tipo_estabelecimento, porte, natureza_juridica,
                    email, telefone_1, telefone_2, data_abertura,
                    data_consulta, status_consulta
                )
                SELECT
                    cnpj, razao_social, nome_fantasia, logradouro, numero,
                    bairro, municipio, uf, cep, situacao_cadastral,
                    tipo_estabelecimento, porte, natureza_juridica,
                    email, telefone_1, telefone_2, data_abertura,
                    data_consulta, status_consulta
                FROM df_tmp
            """)
            buffer.clear()

        if i % LOG_INTERVAL == 0:
            print(f"⏳ {i}/{total}")

        time.sleep(SLEEP_SECONDS)

    con.close()
    print("✅ Enriquecimento finalizado")

# =====================================================
# MAIN
# =====================================================
def main():
    extract_and_load_raw()
    transform_data()
    dados_cnpj()

if __name__ == "__main__":
    main()
