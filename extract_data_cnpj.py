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
SLEEP_SECONDS = 0.6          # ~1,6 requisições/seg (seguro)
MAX_TENTATIVAS = 5
LOG_INTERVAL = 100          # log a cada 100 CNPJs
TABLE_DESTINO = "cno.cnpj_enriquecido"


# =====================================================
# CONEXÃO MOTHERDUCK
# =====================================================
def get_md_connection():
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise ValueError("❌ MOTHERDUCK_TOKEN não encontrado")

    return duckdb.connect(f"md:?motherduck_token={token}")


# =====================================================
# PADRONIZAR CNPJ
# =====================================================
def padronizar_cnpj(cnpj):
    return re.sub(r"\D", "", str(cnpj)).zfill(14)


# =====================================================
# CONSULTA BRASILAPI (COM RETRY + BACKOFF)
# =====================================================
def get_cnpj_info(cnpj):
    tentativa = 0

    while tentativa < MAX_TENTATIVAS:
        tentativa += 1
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
# PROCESSAR CNPJ
# =====================================================
def processar_cnpj(cnpj):
    info = get_cnpj_info(cnpj)
    linhas = []

    if not info:
        return linhas

    base = {
        "cnpj": cnpj,
        "razao_social": info.get("razao_social"),
        "logradouro": info.get("logradouro"),
        "municipio": info.get("municipio"),
        "uf": info.get("uf"),
        "cep": info.get("cep"),
        "situacao_cadastral": info.get("descricao_situacao_cadastral"),
        "tipo_estabelecimento": info.get("descricao_identificador_matriz_filial"),
        "email": info.get("email"),
        "telefone_1": info.get("ddd_telefone_1"),
        "telefone_2": info.get("ddd_telefone_2"),
        "data_consulta": datetime.utcnow()
    }

    # CNAE principal
    linhas.append({
        **base,
        "cnae": info.get("cnae_fiscal"),
        "descricao_cnae": info.get("cnae_fiscal_descricao")
    })

    # CNAEs secundários
    for cnae in info.get("cnaes_secundarios", []):
        linhas.append({
            **base,
            "cnae": cnae.get("codigo"),
            "descricao_cnae": cnae.get("descricao")
        })

    return linhas


# =====================================================
# CRIAR TABELA DESTINO (SE NÃO EXISTIR)
# =====================================================
def criar_tabela_destino(con):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_DESTINO} (
            cnpj VARCHAR,
            razao_social VARCHAR,
            logradouro VARCHAR,
            municipio VARCHAR,
            uf VARCHAR,
            cep VARCHAR,
            situacao_cadastral VARCHAR,
            tipo_estabelecimento VARCHAR,
            email VARCHAR,
            telefone_1 VARCHAR,
            telefone_2 VARCHAR,
            cnae VARCHAR,
            descricao_cnae VARCHAR,
            data_consulta TIMESTAMP
        )
    """)
    print("✅ Tabela destino verificada/criada")


# =====================================================
# MAIN
# =====================================================
def main():
    print("🚀 Iniciando enriquecimento de CNPJs")

    con = get_md_connection()
    criar_tabela_destino(con)

    # -------------------------------------------------
    # BUSCA CNPJs ORIGEM
    # -------------------------------------------------
    df_origem = con.execute("""
        SELECT DISTINCT "NI do responsável"
        FROM cno.cno_vinculos
        WHERE "NI do responsável" IS NOT NULL
    """).df()

    cnpjs = (
        df_origem["NI do responsável"]
        .apply(padronizar_cnpj)
        .unique()
        .tolist()
    )

    total = len(cnpjs)
    print(f"🔎 {total} CNPJs únicos encontrados")

    buffer = []
    processados = 0

    # -------------------------------------------------
    # LOOP PRINCIPAL
    # -------------------------------------------------
    for cnpj in cnpjs:
        linhas = processar_cnpj(cnpj)
        buffer.extend(linhas)

        processados += 1

        # INSERT EM BATCH (a cada 100)
        if len(buffer) >= 100:
            df_insert = pd.DataFrame(buffer)
            con.register("df_tmp", df_insert)
            con.execute(f"INSERT INTO {TABLE_DESTINO} SELECT * FROM df_tmp")
            buffer.clear()

        if processados % LOG_INTERVAL == 0:
            print(f"⏳ {processados}/{total} CNPJs processados")

        time.sleep(SLEEP_SECONDS)

    # INSERT FINAL
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
