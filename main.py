import pandas as pd
import duckdb
import requests
import zipfile
import io
import os
from tqdm import tqdm
from pathlib import Path
#import streamlit as st

DB_PATH = Path(__file__).parent / "cno.duckdb"

#@st.cache_resource
def get_connection():
    return duckdb.connect(database=str(DB_PATH), read_only=True)

def extract_data():
    # URLs e paths
    CNO_URL = "https://arquivos.receitafederal.gov.br/dados/cno/cno.zip"
    DB_FILE = "cno.duckdb"
    TMP_FOLDER = "tmp_csvs"

    # Criar pasta temporária
    os.makedirs(TMP_FOLDER, exist_ok=True)

    print("🔹 Etapa 1: Baixando ZIP...")
    response = requests.get(CNO_URL)
    response.raise_for_status()
    zip_bytes = io.BytesIO(response.content)
    print("✅ Download concluído\n")

    # Conectar ao DuckDB
    print("🔹 Etapa 2: Conectando ao DuckDB...")
    con = duckdb.connect(DB_FILE)
    print(f"✅ Conexão estabelecida em {DB_FILE}\n")

    # Abrir ZIP
    with zipfile.ZipFile(zip_bytes) as z:
        csv_files = [f for f in z.namelist() if f.endswith(".csv")]
        print(f"✅ {len(csv_files)} arquivos CSV encontrados\n")

        for file in tqdm(csv_files, desc="Importando CSVs", unit="arquivo"):
            table_name = file.replace(".csv", "").replace("-", "_")
            print(f"\n➡️ Lendo {file} com Pandas...")

            # Extrair CSV para arquivo temporário
            tmp_path = os.path.join(TMP_FOLDER, file).replace("\\", "/")
            os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
            with z.open(file) as f_in, open(tmp_path, "wb") as f_out:
                f_out.write(f_in.read())

            # Ler CSV com Pandas, ignorando linhas inválidas
            df = pd.read_csv(
                tmp_path,
                sep=',',
                dtype=str,
                encoding='latin1',
                on_bad_lines='skip'
            )

            # Enviar para DuckDB
            con.register("tmp_df", df)
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM tmp_df")
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

            print(f"✅ {table_name} carregada ({count:,} linhas)")

    print("\n🎉 Todos os arquivos importados com sucesso!")
    con.close()
    print("🔹 Conexão com DuckDB encerrada.")

def transform_data():

    # Query todas as tabelas em df
    con = get_connection()
    cno_area = con.execute(f"SELECT * FROM cno_areas").df()
    cno = con.execute(f"SELECT * FROM cno").df()
    cno_vinculos = con.execute(f"SELECT * FROM cno_vinculos").df()
    cno_cnaes = con.execute(f"SELECT * FROM cno_cnaes").df()

    # Em cno_areas, há dados repetidos
    # Pegar o CNO com o maior índice
    cno_area = (
        cno_area
        .sort_index()
        .drop_duplicates(subset='CNO', keep='last')
    )
    
    print(cno['CNO'].duplicated().any())
    print(cno_area['CNO'].duplicated().any())
    print(cno_vinculos['CNO'].duplicated().any())
    print(cno_cnaes['CNO'].duplicated().any())

    # Agrupar cno e cno_area
    cno_base = cno.merge(cno_area, on='CNO', how='left', suffixes=('', '_area'))
    print(f"CNO base shape: {cno_base.head()}")

# extract_data()
transform_data()