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
# CONFIGURATIONS
# =====================================================
CNO_URL = "https://arquivos.receitafederal.gov.br/index.php/s/PC6732BXG9B98W3/download?path=%2F&files=cno.zip"

TABLE_EMPRESA = "cnpj"
TABLE_SOCIOS = "cnpj_socios"

SLEEP_SECONDS = 0.5   
MAX_TENTATIVAS_PADRAO = 3
BATCH_SIZE = 50       
MAX_WORKERS = 10      

# =====================================================
# CONNECTION & UTILS
# =====================================================
def get_md_connection():
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise ValueError("❌ MOTHERDUCK_TOKEN não encontrado no ambiente ou arquivo .env")
    con = duckdb.connect(f"md:cno?motherduck_token={token}")
    con.execute("USE main")
    return con

def padronizar_cnpj(cnpj):
    return re.sub(r"\D", "", str(cnpj)).zfill(14)

def clean_digits(v):
    return re.sub(r"\D", "", str(v)) if v and str(v).strip() else None

def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

# =====================================================
# API CALLS
# =====================================================
def get_cnpj_info(cnpj, max_retries):
    """Queries BrasilAPI with custom retry logic and exponential backoff."""
    for tentativa in range(1, max_retries + 1):
        try:
            r = requests.get(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}", timeout=15)
            if r.status_code == 200: 
                return r.json()
            if r.status_code == 404: 
                return {"_status": "nao_encontrado"}
            if r.status_code == 429: # Rate limit
                time.sleep(tentativa * 5)
            else:
                time.sleep(tentativa * 2)
        except Exception:
            time.sleep(tentativa * 2)
    return {"_status": "erro"}

# =====================================================
# DATA EXTRACTION & TRANSFORM (CNO)
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

def transform_data():
    con = get_md_connection()
    con.execute("USE cno")
    con.execute("""
        CREATE OR REPLACE TABLE base_cno AS
        SELECT c.*, a.* EXCLUDE (CNO)
        FROM cno c
        LEFT JOIN (SELECT *, ROW_NUMBER() OVER (PARTITION BY CNO ORDER BY rowid DESC) rn FROM cno_areas) a ON c.CNO = a.CNO AND a.rn = 1
    """)
    print(f"✅ base_cno criada/atualizada", flush=True)
    con.close()

# =====================================================
# CNPJ PROCESSING LOGIC
# =====================================================
def criar_tabelas_destino(con):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_EMPRESA} (
            cnpj VARCHAR PRIMARY KEY, 
            razao_social VARCHAR, 
            nome_fantasia VARCHAR, 
            capital_social DOUBLE, 
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

def processar_cnpj(cnpj, max_retries):
    info = get_cnpj_info(cnpj, max_retries)
    
    if "_status" in info:
        return ({"cnpj": cnpj, "data_consulta": datetime.utcnow(), "status_consulta": info["_status"]}, [])

    empresa = {
        "cnpj": cnpj, 
        "razao_social": info.get("razao_social"), 
        "nome_fantasia": info.get("nome_fantasia"),
        "capital_social": float(info.get("capital_social", 0)),
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

def normalize_and_load(con, table_name, df, mode="IGNORE"):
    """Trata tipos de dados e realiza a inserção conforme a restrição da tabela."""
    if df.empty: return

    # Forçar tipos de dados para evitar erros de conversão no DuckDB
    for col in df.columns:
        if "data_" in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        elif col == "capital_social":
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype(float)
        else:
            df[col] = df[col].astype(str).replace(['None', 'nan', '<NA>', 'NaN'], None)

    temp_name = f"tmp_{table_name}_{int(time.time() * 1000)}" # Usando milissegundos para evitar conflito
    con.register(temp_name, df)
    
    # Lógica de inserção
    if table_name == TABLE_EMPRESA:
        # A tabela de empresas TEM Primary Key, então podemos usar OR IGNORE / OR REPLACE
        if mode == "IGNORE":
            con.execute(f"INSERT OR IGNORE INTO cno.main.{table_name} SELECT * FROM {temp_name}")
        elif mode == "REPLACE":
            con.execute(f"INSERT OR REPLACE INTO cno.main.{table_name} SELECT * FROM {temp_name}")
    else:
        # A tabela de sócios NÃO TEM Primary Key. 
        # Como já rodamos o DELETE antes no reprocessamento, um INSERT simples resolve.
        con.execute(f"INSERT INTO cno.main.{table_name} SELECT * FROM {temp_name}")
    
    con.unregister(temp_name)

# =====================================================
# MAIN WORKFLOWS
# =====================================================
def dados_cnpj():
    """Fetches and inserts new CNPJs that aren't in the database yet."""
    con = get_md_connection()
    criar_tabelas_destino(con)

    df_faltantes = con.execute(f"""
        SELECT DISTINCT regexp_replace("NI do responsável", '\\D', '', 'g') cnpj
        FROM cno.cno_base
        WHERE "NI do responsável" IS NOT NULL
          AND cnpj NOT IN (SELECT cnpj FROM cno.main.{TABLE_EMPRESA})
    """).df()

    cnpjs = df_faltantes["cnpj"].dropna().tolist()
    total = len(cnpjs)
    print(f"🔎 {total} CNPJs novos para processar", flush=True)

    for i in range(0, total, BATCH_SIZE):
        batch = cnpjs[i : i + BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            resultados = list(executor.map(lambda c: processar_cnpj(padronizar_cnpj(c), MAX_TENTATIVAS_PADRAO), batch))

        lista_e = [r[0] for r in resultados if r[0]]
        lista_s = [s for r in resultados for s in r[1]]

        if lista_e: normalize_and_load(con, TABLE_EMPRESA, pd.DataFrame(lista_e), "IGNORE")
        if lista_s: normalize_and_load(con, TABLE_SOCIOS, pd.DataFrame(lista_s), "IGNORE")

        print(f"⏳ Progresso: {min(i + BATCH_SIZE, total)}/{total}", flush=True)
        time.sleep(SLEEP_SECONDS)
    con.close()

def reprocessar_erros():
    """Identifica falhas anteriores e tenta novamente com prioridade (10 tentativas)."""
    con = get_md_connection()
    df_erros = con.execute(f"SELECT cnpj FROM cno.main.{TABLE_EMPRESA} WHERE status_consulta = 'erro'").df()
    cnpjs_falhos = df_erros["cnpj"].tolist()
    total = len(cnpjs_falhos)
    
    if total == 0:
        print("✅ Nenhum CNPJ com status 'erro' para reprocessar.")
        con.close()
        return

    print(f"🔄 Reprocessando {total} erros com limite de 10 tentativas...", flush=True)

    for i in range(0, total, BATCH_SIZE):
        batch = cnpjs_falhos[i : i + BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            resultados = list(executor.map(lambda c: processar_cnpj(c, 10), batch))

        lista_e = [r[0] for r in resultados if r[0]]
        lista_s = [s for r in resultados for s in r[1]]

        if lista_e:
            normalize_and_load(con, TABLE_EMPRESA, pd.DataFrame(lista_e), "REPLACE")

        if lista_s:
            df_s = pd.DataFrame(lista_s)
            # --- CORREÇÃO AQUI ---
            # Pegamos os CNPJs únicos do lote e formatamos como uma string para o SQL: 'cnpj1', 'cnpj2'
            cnpjs_unicos = df_s['cnpj'].unique().tolist()
            cnpjs_sql = ", ".join([f"'{c}'" for c in cnpjs_unicos])
            
            # Deletamos os sócios antigos antes de inserir os novos para evitar duplicidade
            con.execute(f"DELETE FROM cno.main.{TABLE_SOCIOS} WHERE cnpj IN ({cnpjs_sql})")
            # ---------------------
            
            normalize_and_load(con, TABLE_SOCIOS, df_s, "IGNORE")

        print(f"⏳ Reprocessamento: {min(i + BATCH_SIZE, total)}/{total}", flush=True)
        time.sleep(SLEEP_SECONDS)
    
    con.close()
    print("✅ Reprocessamento finalizado!")

# =====================================================
# EXECUTION
# =====================================================
if __name__ == "__main__":
    # 1. Update Raw Data
    extract_and_load_raw(threads=8)
    transform_data()
    
    # 2. Process new entries
    dados_cnpj()
    
    # 3. Retry previous failures (Rewrite data)
    reprocessar_erros()