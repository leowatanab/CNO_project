# =====================================================
# IMPORTS
# =====================================================
import duckdb
import pandas as pd
import os
import re
import requests
import time
from datetime import datetime


# =====================================================
# CONFIGURAÇÕES
# =====================================================
SLEEP_SECONDS = 0.6
MAX_TENTATIVAS = 5
BATCH_SIZE = 100
LOG_INTERVAL = 100
TABLE_DESTINO = "cno.cnpj_cadastral"


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
# CONSULTA BRASILAPI (RETRY + BACKOFF)
# =====================================================
def get_cnpj_info(cnpj):
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            r = requests.get(
                f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}",
                timeout=10
            )

            if r.status_code == 200:
                return r.json()

            if r.status_code == 404:
                return None

            time.sleep(min(tentativa * 2, 10))

        except requests.exceptions.RequestException:
            time.sleep(min(tentativa * 2, 10))

    return None


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
        CREATE INDEX IF NOT EXISTS idx_cnpj_cadastral_cnpj
        ON {TABLE_DESTINO}(cnpj)
    """)

    print("✅ Tabela cnpj_cadastral pronta")


# =====================================================
# PROCESSAR CNPJ (1 LINHA)
# =====================================================
def processar_cnpj(cnpj):
    info = get_cnpj_info(cnpj)

    if not info:
        return {
            "cnpj": cnpj,
            "data_consulta": datetime.utcnow(),
            "status_consulta": "erro_api"
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
        "data_abertura": info.get("data_inicio_atividade"),
        "data_consulta": datetime.utcnow(),
        "status_consulta": "ok"
    }


# =====================================================
# MAIN
# =====================================================
def main():
    print("🚀 Iniciando enriquecimento cadastral de CNPJs")

    con = get_md_connection()
    criar_tabela_destino(con)

    # -------------------------------------------------
    # BUSCAR SOMENTE CNPJs AINDA NÃO PROCESSADOS
    # -------------------------------------------------
    df_origem = con.execute(f"""
        SELECT DISTINCT
            regexp_replace("NI do responsável", '\\D', '', 'g') AS cnpj
        FROM cno.cno_vinculos
        WHERE "NI do responsável" IS NOT NULL
          AND regexp_replace("NI do responsável", '\\D', '', 'g') NOT IN (
              SELECT cnpj FROM {TABLE_DESTINO}
          )
    """).df()

    cnpjs = df_origem["cnpj"].dropna().unique().tolist()
    total = len(cnpjs)

    print(f"🔎 {total} CNPJs novos encontrados")

    buffer = []

    # -------------------------------------------------
    # LOOP PRINCIPAL
    # -------------------------------------------------
    for i, cnpj in enumerate(cnpjs, start=1):
        cnpj = padronizar_cnpj(cnpj)
        row = processar_cnpj(cnpj)
        buffer.append(row)

        if len(buffer) >= BATCH_SIZE:
            df_insert = pd.DataFrame(buffer)
            con.register("df_tmp", df_insert)
            con.execute(f"INSERT INTO {TABLE_DESTINO} SELECT * FROM df_tmp")
            buffer.clear()

        if i % LOG_INTERVAL == 0:
            print(f"⏳ {i}/{total} CNPJs processados")

        time.sleep(SLEEP_SECONDS)

    # -------------------------------------------------
    # INSERT FINAL
    # -------------------------------------------------
    if buffer:
        df_insert = pd.DataFrame(buffer)
        con.register("df_tmp", df_insert)
        con.execute(f"INSERT INTO {TABLE_DESTINO} SELECT * FROM df_tmp")

    con.close()
    print("✅ Processo finalizado com sucesso")


# =====================================================
# ENTRYPOINT
# =====================================================
if __name__ == "__main__":
    main()
