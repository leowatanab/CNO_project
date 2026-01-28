import duckdb
import pandas as pd
import os
import re
import requests
import time
from datetime import datetime, timedelta


# =====================================================
# CONFIGURAÇÕES
# =====================================================
SLEEP_SECONDS = 0.6
MAX_TENTATIVAS = 5
BATCH_SIZE = 100
LOG_INTERVAL = 100
RETRY_DAYS = 7

TABLE_DESTINO = "cno.cnpj_cadastral"
TABLE_ORIGEM = "cno.cno_vinculos"


# =====================================================
# CONEXÃO MOTHERDUCK
# =====================================================
def get_md_connection():
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise ValueError("❌ MOTHERDUCK_TOKEN não encontrado")

    return duckdb.connect(f"md:?motherduck_token={token}")


# =====================================================
# UTILIDADES
# =====================================================
def padronizar_cnpj(cnpj):
    return re.sub(r"\D", "", str(cnpj)).zfill(14)


def clean_digits(v):
    return re.sub(r"\D", "", v) if v else None


# =====================================================
# HTTP SESSION (PERFORMANCE)
# =====================================================
session = requests.Session()
session.headers.update({"User-Agent": "cnpj-enrichment/1.0"})


# =====================================================
# CONSULTA BRASILAPI (RETRY + BACKOFF)
# =====================================================
def get_cnpj_info(cnpj):
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            r = session.get(
                f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}",
                timeout=10
            )

            if r.status_code == 200:
                return r.json()

            if r.status_code == 404:
                return {"_status": "cnpj_nao_encontrado"}

            if r.status_code == 429:
                time.sleep(10)
                continue

            time.sleep(min(tentativa * 2, 10))

        except requests.exceptions.Timeout:
            return {"_status": "timeout"}

        except requests.exceptions.RequestException as e:
            return {"_status": f"erro_api: {str(e)[:100]}"}

    return {"_status": "erro_api"}


# =====================================================
# CRIAR TABELA DESTINO
# =====================================================
def criar_tabela_destino(con):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_DESTINO} (
            cnpj VARCHAR,
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

    con.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_cnpj_cadastral_cnpj
        ON {TABLE_DESTINO}(cnpj)
    """)

    print("✅ Tabela destino pronta")


# =====================================================
# QUERY INCREMENTAL
# =====================================================
def buscar_cnpjs_incrementais(con) -> list[str]:
    df = con.execute(f"""
        SELECT DISTINCT
            regexp_replace(o."NI do responsável", '\\D', '', 'g') AS cnpj
        FROM {TABLE_ORIGEM} o
        LEFT JOIN {TABLE_DESTINO} d
          ON regexp_replace(o."NI do responsável", '\\D', '', 'g') = d.cnpj
        WHERE o."NI do responsável" IS NOT NULL
          AND (
                d.cnpj IS NULL
             OR (
                    d.status_consulta IN ('timeout', 'erro_api')
                AND d.data_consulta < CURRENT_DATE - INTERVAL {RETRY_DAYS} DAY
             )
          )
    """).df()

    return df["cnpj"].dropna().unique().tolist()


# =====================================================
# PROCESSAR CNPJ
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
            datetime.strptime(info.get("data_inicio_atividade"), "%Y-%m-%d").date()
            if info.get("data_inicio_atividade")
            else None
        ),
        "data_consulta": datetime.utcnow(),
        "status_consulta": "ok"
    }


# =====================================================
# MAIN
# =====================================================
def main():
    print("🚀 Job incremental de enriquecimento de CNPJ iniciado")

    con = get_md_connection()
    criar_tabela_destino(con)

    cnpjs = buscar_cnpjs_incrementais(con)
    total = len(cnpjs)

    print(f"🔎 {total} CNPJs pendentes para processamento")

    buffer = []

    for i, cnpj in enumerate(cnpjs, start=1):
        row = processar_cnpj(padronizar_cnpj(cnpj))
        buffer.append(row)

        if len(buffer) >= BATCH_SIZE:
            con.register("df_tmp", pd.DataFrame(buffer))
            con.execute(f"""
                INSERT OR IGNORE INTO {TABLE_DESTINO} (
                    cnpj, razao_social, nome_fantasia, logradouro, numero, bairro,
                    municipio, uf, cep, situacao_cadastral, tipo_estabelecimento,
                    porte, natureza_juridica, email, telefone_1, telefone_2,
                    data_abertura, data_consulta, status_consulta
                )
                SELECT
                    cnpj, razao_social, nome_fantasia, logradouro, numero, bairro,
                    municipio, uf, cep, situacao_cadastral, tipo_estabelecimento,
                    porte, natureza_juridica, email, telefone_1, telefone_2,
                    data_abertura, data_consulta, status_consulta
                FROM df_tmp
            """)
            buffer.clear()

        if i % LOG_INTERVAL == 0:
            print(f"⏳ {i}/{total} CNPJs processados")

        time.sleep(SLEEP_SECONDS)

    if buffer:
        con.register("df_tmp", pd.DataFrame(buffer))
        con.execute("""
            INSERT OR IGNORE INTO {TABLE_DESTINO}
            SELECT * FROM df_tmp
        """)

    con.close()
    print("✅ Job incremental finalizado com sucesso")


if __name__ == "__main__":
    main()
