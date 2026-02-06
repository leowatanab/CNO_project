import os
import re
import time
import requests
import duckdb
import pandas as pd
import tempfile
import shutil
import zipfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# =====================================================
# CONFIGURAÇÕES
# =====================================================
CNO_URL = "https://arquivos.receitafederal.gov.br/index.php/s/PC6732BXG9B98W3/download?path=%2F&files=cno.zip"

TABLE_EMPRESA = "cnpj"
TABLE_SOCIOS = "cnpj_socios"

SLEEP_SECONDS = 0.5   
MAX_TENTATIVAS = 3
BATCH_SIZE = 50       
MAX_WORKERS = 10      



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
# APOIO CNPJ
# =====================================================
def padronizar_cnpj(cnpj):
    return re.sub(r"\D", "", str(cnpj)).zfill(14)

def clean_digits(v):
    return re.sub(r"\D", "", str(v)) if v and str(v).strip() else None

def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

# =====================================================
# EXTRAÇÃO + CARGA INCREMENTAL DO CNO
# =====================================================
def extract_and_load_raw(threads: int = 8):
    workdir = tempfile.mkdtemp(prefix="cno_")
    zip_path = os.path.join(workdir, "cno.zip")
    try:
        print("⬇️ Baixando CNO...", flush=True)
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
                if not file.lower().endswith(".csv"): continue
                table = os.path.basename(file).replace(".csv", "").replace("-", "_").lower()
                print(f"📄 {file} → {table}")
                
                csv_path = os.path.join(workdir, file)
                os.makedirs(os.path.dirname(csv_path), exist_ok=True)
                
                with z.open(file) as src:
                    data = src.read()
                
                try:
                    decoded = data.decode("utf-8")
                except UnicodeDecodeError:
                    decoded = data.decode("latin-1", errors="replace")
                
                with open(csv_path, "w", encoding="utf-8") as f:
                    f.write(decoded)

                con.execute(f"CREATE TABLE IF NOT EXISTS {qi(table)} AS SELECT * FROM read_csv_auto('{csv_path}', ALL_VARCHAR=TRUE)")
        con.close()
        print("✅ CNO carregado", flush=True)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

# =====================================================
# TRANSFORMAÇÃO
# =====================================================
def transform_data():
    con = get_md_connection()
    con.execute("USE cno")
    con.execute("""
        CREATE OR REPLACE TABLE base_cno AS
        SELECT c.*, a.* EXCLUDE (CNO), v.* EXCLUDE (CNO)
        FROM cno c
        LEFT JOIN (SELECT *, ROW_NUMBER() OVER (PARTITION BY CNO ORDER BY rowid DESC) rn FROM cno_areas) a ON c.CNO = a.CNO AND a.rn = 1
    """)
    print(f"✅ cno_base criada", flush=True)
    con.close()

def get_cnpj_info(cnpj):
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            r = requests.get(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}", timeout=15)
            if r.status_code == 200: return r.json()
            if r.status_code == 404: return {"_status": "nao_encontrado"}
            time.sleep(tentativa * 2)
        except:
            time.sleep(tentativa * 2)
    return {"_status": "erro"}

# =====================================================
# CRIAÇÃO DAS TABELAS (INCLUINDO CAPITAL SOCIAL)
# =====================================================
def criar_tabelas_destino(con):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_EMPRESA} (
            cnpj VARCHAR PRIMARY KEY, 
            razao_social VARCHAR, 
            nome_fantasia VARCHAR, 
            capital_social DOUBLE,  -- Adicionado
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
        );
        CREATE TABLE IF NOT EXISTS {TABLE_SOCIOS} (
            cnpj VARCHAR, 
            nome_socio VARCHAR, 
            cnpj_cpf_do_socio VARCHAR, 
            qualificacao_socio VARCHAR, 
            data_entrada_sociedade DATE, 
            faixa_etaria VARCHAR
        );
    """)
    print(f"✅ Tabelas '{TABLE_EMPRESA}' e '{TABLE_SOCIOS}' prontas", flush=True)

def processar_cnpj(cnpj):
    info = get_cnpj_info(cnpj)
    if "_status" in info:
        return ({"cnpj": cnpj, "data_consulta": datetime.utcnow(), "status_consulta": info["_status"]}, [])

    empresa = {
        "cnpj": cnpj, 
        "razao_social": info.get("razao_social"), 
        "nome_fantasia": info.get("nome_fantasia"),
        "capital_social": float(info.get("capital_social", 0)), # Capturando capital social
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
        "data_abertura": pd.to_datetime(info.get("data_inicio_atividade")).date() if info.get("data_inicio_atividade") else None,
        "data_consulta": datetime.utcnow(), 
        "status_consulta": "ok"
    }

    socios = []
    for s in info.get("qsa", []):
        socios.append({
            "cnpj": cnpj, 
            "nome_socio": s.get("nome_socio"), 
            "cnpj_cpf_do_socio": s.get("cnpj_cpf_do_socio"),
            "qualificacao_socio": s.get("qualificacao_socio"),
            "data_entrada_sociedade": pd.to_datetime(s.get("data_entrada_sociedade")).date() if s.get("data_entrada_sociedade") else None,
            "faixa_etaria": s.get("faixa_etaria")
        })
    return (empresa, socios)


def dados_cnpj():
    con = get_md_connection()
    criar_tabelas_destino(con)

    # Busca CNPJs que estão na cno_base mas não na nossa tabela final
    df_faltantes = con.execute(f"""
        SELECT DISTINCT regexp_replace("NI do responsável", '\\D', '', 'g') cnpj
        FROM cno.cno_base
        WHERE "NI do responsável" IS NOT NULL
          AND cnpj NOT IN (SELECT cnpj FROM {TABLE_EMPRESA})
    """).df()

    cnpjs = df_faltantes["cnpj"].dropna().tolist()
    total = len(cnpjs)
    print(f"🔎 {total} CNPJs novos para processar", flush=True)

    for i in range(0, total, BATCH_SIZE):
        batch = cnpjs[i : i + BATCH_SIZE]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            resultados = list(executor.map(lambda c: processar_cnpj(padronizar_cnpj(c)), batch))

        lista_e = [r[0] for r in resultados if r[0]]
        lista_s = [s for r in resultados for s in r[1]]

        # --- TRATAMENTO PARA TABELA DE EMPRESAS ---
        if lista_e:
            df_e = pd.DataFrame(lista_e)
            cols_e = ["cnpj", "razao_social", "nome_fantasia", "capital_social", "logradouro", "numero", "bairro", "municipio", "uf", "cep", "situacao_cadastral", "tipo_estabelecimento", "porte", "natureza_juridica", "email", "telefone_1", "telefone_2", "data_abertura", "data_consulta", "status_consulta"]
            
            for c in cols_e:
                if c not in df_e.columns:
                    df_e[c] = None
                
                # Força tipo String para campos de texto (evita erro 'str' not recognized)
                if c not in ["capital_social", "data_abertura", "data_consulta"]:
                    df_e[c] = df_e[c].astype(str).replace(['None', 'nan', '<NA>', 'NaN'], None)
                
            # Garante que capital_social seja float
            df_e["capital_social"] = pd.to_numeric(df_e["capital_social"], errors='coerce').fillna(0.0)

            con.register("tmp_e", df_e[cols_e])
            con.execute(f"INSERT OR IGNORE INTO {TABLE_EMPRESA} SELECT * FROM tmp_e")
            con.unregister("tmp_e")

        # --- TRATAMENTO PARA TABELA DE SÓCIOS ---
        if lista_s:
            df_s = pd.DataFrame(lista_s)
            cols_s = ["cnpj", "nome_socio", "cnpj_cpf_do_socio", "qualificacao_socio", "data_entrada_sociedade", "faixa_etaria"]
            
            for c in cols_s:
                if c not in ["data_entrada_sociedade"]:
                    df_s[c] = df_s[c].astype(str).replace(['None', 'nan', '<NA>', 'NaN'], None)
            
            con.register("tmp_s", df_s[cols_s])
            con.execute(f"INSERT INTO {TABLE_SOCIOS} SELECT * FROM tmp_s")
            con.unregister("tmp_s")

        # Log de progresso com flush para o GitHub Actions
        print(f"⏳ Processado: {min(i + BATCH_SIZE, total)}/{total}", flush=True)
        time.sleep(SLEEP_SECONDS)

    con.close()
    print(f"✅ Enriquecimento da tabela '{TABLE_EMPRESA}' finalizado!", flush=True)

if __name__ == "__main__":
    #extract_and_load_raw(threads=8)
    #transform_data()
<<<<<<< HEAD
    dados_cnpj()
=======
    dados_cnpj()
>>>>>>> 212813d156ed7672de825fb6315de26dd9540cf6
