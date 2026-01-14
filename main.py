import pandas as pd
import duckdb
import requests
import zipfile
import io
import os
from tqdm import tqdm

# =====================================================
# CONEXÃO MOTHERDUCK
# =====================================================
def get_md_connection():
    md_token = os.getenv("MOTHERDUCK_TOKEN")
    if not md_token:
        raise ValueError("❌ MOTHERDUCK_TOKEN não encontrado")

    return duckdb.connect(f"md:?motherduck_token={md_token}")

# =====================================================
# EXTRAÇÃO + CARGA RAW
# =====================================================
def extract_and_load_raw():
    CNO_URL = "https://arquivos.receitafederal.gov.br/dados/cno/cno.zip"

    print("🔹 Baixando ZIP do CNO...")
    response = requests.get(CNO_URL, timeout=180)
    response.raise_for_status()
    zip_bytes = io.BytesIO(response.content)
    print("✅ Download concluído")

    con = get_md_connection()

    # Criar database e usar
    con.execute("CREATE DATABASE IF NOT EXISTS cno")
    con.execute("USE cno")

    with zipfile.ZipFile(zip_bytes) as z:
        csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
        print(f"✅ {len(csv_files)} arquivos CSV encontrados")

        for file in tqdm(csv_files, desc="Importando CSVs"):
            table_name = file.replace(".csv", "").replace("-", "_").lower()

            print(f"➡️ Importando {file} → {table_name}")

            with z.open(file) as f:
                df = pd.read_csv(
                    f,
                    sep=",",
                    dtype=str,
                    encoding="latin1",
                    on_bad_lines="skip"
                )

            con.register("tmp_df", df)

            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM tmp_df
            """)

            count = con.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]

            print(f"✅ {table_name} criada ({count:,} linhas)")

    con.close()
    print("🎉 Tabelas criadas")

# =====================================================
# TRANSFORMAÇÃO (SQL PURO)
# =====================================================
def transform_data():
    print("🔹 Iniciando transformação dos dados")

    con = get_md_connection()
    con.execute("USE cno")

    # Tabela final deduplicada e unificada
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
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY CNO
                           ORDER BY rowid DESC
                       ) AS rn
                FROM cno_areas
            )
            WHERE rn = 1
        ) a USING (CNO)
        LEFT JOIN (
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY CNO
                           ORDER BY rowid DESC
                       ) AS rn
                FROM cno_vinculos
            )
            WHERE rn = 1
        ) v USING (CNO)
    """)

    total = con.execute("SELECT COUNT(*) FROM cno_base").fetchone()[0]
    print(f"✅ cno_base criada ({total:,} registros)")

    con.close()
    print("🎉 Transformação concluída")

# =====================================================
# MAIN
# =====================================================
def main():
    extract_and_load_raw()
    transform_data()

if __name__ == "__main__":
    main()
