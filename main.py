
import os
import io
import zipfile
import tempfile
import requests
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm
import shutil
import duckdb

# Conect to MotherDuck
def get_md_connection():
    md_token = os.getenv("MOTHERDUCK_TOKEN")
    if not md_token:
        raise ValueError("❌ MOTHERDUCK_TOKEN não encontrado")

    return duckdb.connect(f"md:?motherduck_token={md_token}")


def extract_and_load_raw(
    table_keys: dict | None = None,    # mapeamento opcional: {'nome_tabela': ['col1', 'col2', ...]}
    threads: int = 8,                  # threads para o parser do DuckDB
    force_full_reload: bool = False    # se True, recria as tabelas do zero
):
    """
    Baixa e carrega o CNO de forma otimizada e incremental.

    :param table_keys: dict opcional de chaves por tabela (para MERGE/UPSERT). Ex.: {'cno': ['CNO']}
    :param threads: número de threads para o parser do DuckDB.
    :param force_full_reload: se True, descarta incremental e recria tudo.
    """
    CNO_URL = "https://arquivos.receitafederal.gov.br/dados/cno/cno.zip"

    # ---- Helpers ----
    def download_with_progress(url: str, dest_path: str, timeout=180, chunk_size=4 * 1024 * 1024):
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"]
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        session.mount("http://", HTTPAdapter(max_retries=retries))

        total = None
        try:
            head = session.head(url, timeout=30)
            if head.ok and "Content-Length" in head.headers:
                total = int(head.headers["Content-Length"])
        except Exception:
            pass

        print("🔹 Baixando ZIP do CNO (streaming + retries)...")
        with session.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            with open(dest_path, "wb") as f, tqdm(
                total=total, unit="B", unit_scale=True, unit_divisor=1024,
                desc="Baixando cno.zip", leave=True
            ) as pbar:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        if total:
                            pbar.update(len(chunk))
        print("✅ Download concluído:", dest_path)

    def qi(name: str) -> str:
        """Quoting seguro para nomes de colunas/tabelas (com acentos, espaços)."""
        return '"' + name.replace('"', '""') + '"'

    def table_exists(con, name: str) -> bool:
        return con.execute("SELECT 1 FROM information_schema.tables WHERE table_schema=current_schema() AND table_name=?", [name]).fetchone() is not None

    def get_columns(con, table: str) -> list[str]:
        # Funciona para tabelas normais; para temporárias também costuma funcionar
        rows = con.execute(f"PRAGMA table_info({qi(table)})").fetchall()
        # PRAGMA table_info retorna: (cid, name, type, notnull, dflt_value, pk)
        return [r[1] for r in rows]

    def add_missing_columns(con, table: str, missing_cols: list[str]):
        for col in missing_cols:
            con.execute(f"ALTER TABLE {qi(table)} ADD COLUMN {qi(col)} VARCHAR")

    # Diretórios temporários
    workdir = tempfile.mkdtemp(prefix="cno_work_")
    zip_path = os.path.join(workdir, "cno.zip")
    utf8_dir = os.path.join(workdir, "utf8_csvs")
    os.makedirs(utf8_dir, exist_ok=True)

    DEFAULT_KEYS = {
        # Se você souber mais chaves por tabela, acrescente aqui:
        # 'cno': ['CNO'],  # exemplo recomendado
    }
    if table_keys is None:
        table_keys = DEFAULT_KEYS.copy()
    else:
        # mescla defaults com chaves fornecidas
        # prioridade para as chaves do usuário
        merged = DEFAULT_KEYS.copy()
        merged.update(table_keys)
        table_keys = merged

    try:
        # 1) Download
        download_with_progress(CNO_URL, zip_path)

        # 2) Abrir ZIP e listar CSVs
        with zipfile.ZipFile(zip_path) as z:
            csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
            print(f"✅ {len(csv_files)} arquivos CSV encontrados no ZIP")

            # 3) Conexão e sessão DuckDB
            con = get_md_connection()
            con.execute("CREATE DATABASE IF NOT EXISTS cno")
            con.execute("USE cno")
            con.execute(f"PRAGMA threads = {int(threads)}")

            for file in tqdm(csv_files, desc="Importando CSVs", unit="arquivo"):
                # Nome de tabela baseado no arquivo
                table_name = (
                    file.replace("\\", "/").split("/")[-1]
                        .replace(".csv", "")
                        .replace("-", "_")
                        .replace(" ", "_")
                        .lower()
                )

                print(f"➡️ Processando {file} → {table_name}")

                # 4) Detectar encoding (amostra)
                with z.open(file) as fpeek:
                    head_bytes = fpeek.read(131072)
                encoding = "utf-8"
                try:
                    head_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    encoding = "latin-1"

                # 5) Extrair/transcodificar para UTF-8
                out_path = os.path.join(utf8_dir, os.path.basename(file))
                if encoding == "utf-8":
                    with z.open(file) as src, open(out_path, "wb") as dst:
                        shutil.copyfileobj(src, dst, length=4 * 1024 * 1024)
                else:
                    with z.open(file) as src, \
                         io.TextIOWrapper(src, encoding="latin-1", errors="replace", newline="") as txt_in, \
                         open(out_path, "w", encoding="utf-8", newline="") as txt_out:
                        shutil.copyfileobj(txt_in, txt_out, length=1 * 1024 * 1024)

                # 6) Carregar CSV em staging temporária
                staging = f"{table_name}__staging"
                con.execute(f"DROP TABLE IF EXISTS {qi(staging)}")
                con.execute(f"""
                    CREATE TEMP TABLE {qi(staging)} AS
                    SELECT *
                    FROM read_csv_auto(?, HEADER=TRUE, SEP=',', ALL_VARCHAR=TRUE, IGNORE_ERRORS=TRUE);
                """, [out_path])

                # 7) Criar/atualizar tabela final
                if force_full_reload or not table_exists(con, table_name):
                    if force_full_reload and table_exists(con, table_name):
                        con.execute(f"DROP TABLE {qi(table_name)}")
                    con.execute(f"CREATE TABLE {qi(table_name)} AS SELECT * FROM {qi(staging)}")
                    count = con.execute(f"SELECT COUNT(*) FROM {qi(table_name)}").fetchone()[0]
                    print(f"✅ {table_name} criada do zero ({count:,} linhas) — encoding: {encoding}")
                    continue

                # ---- Incremental ----
                # alinhar schemas (adiciona colunas novas do staging no destino)
                target_cols = set(get_columns(con, table_name))
                staging_cols = get_columns(con, staging)

                missing_in_target = [c for c in staging_cols if c not in target_cols]
                if missing_in_target:
                    add_missing_columns(con, table_name, missing_in_target)
                    # atualizar a lista após ALTERs
                    target_cols = set(get_columns(con, table_name))

                # Colunas em comum (ordem = do staging, para consistência)
                common_cols = [c for c in staging_cols if c in target_cols]

                # Se houver chaves para essa tabela, faremos MERGE (UPSERT)
                keys = table_keys.get(table_name)
                if keys:
                    # Verifica se todas as chaves existem nas colunas em comum
                    missing_keys = [k for k in keys if k not in common_cols]
                    if missing_keys:
                        print(f"⚠️ Chaves {missing_keys} não encontradas em {table_name}; usando modo 'append_novos' (EXCEPT).")
                        keys = None

                if keys:
                    # MERGE/UPSERT
                    non_key_cols = [c for c in common_cols if c not in keys]
                    if not non_key_cols:
                        # se todas colunas são chave, o UPDATE não faz sentido; vamos apenas evitar duplicatas no insert
                        # Inserir apenas quando não existe pela chave:
                        on_clause = " AND ".join([f"t.{qi(k)} = s.{qi(k)}" for k in keys])
                        insert_cols = common_cols  # inserimos todas as comuns
                        insert_list = ", ".join(qi(c) for c in insert_cols)
                        values_list = ", ".join(f"s.{qi(c)}" for c in insert_cols)
                        con.execute(f"""
                            INSERT INTO {qi(table_name)} ({insert_list})
                            SELECT {values_list}
                            FROM {qi(staging)} s
                            WHERE NOT EXISTS (
                                SELECT 1 FROM {qi(table_name)} t
                                WHERE {on_clause}
                            );
                        """)
                    else:
                        on_clause = " AND ".join([f't.{qi(k)} = s.{qi(k)}' for k in keys])
                        set_clause = ", ".join([f't.{qi(c)} = s.{qi(c)}' for c in non_key_cols])

                        # Para o INSERT, usaremos as colunas comuns (chaves + não-chaves)
                        insert_cols = common_cols
                        insert_list = ", ".join(qi(c) for c in insert_cols)
                        values_list = ", ".join(f"s.{qi(c)}" for c in insert_cols)

                        con.execute(f"""
                            MERGE INTO {qi(table_name)} t
                            USING {qi(staging)} s
                            ON {on_clause}
                            WHEN MATCHED THEN UPDATE SET {set_clause}
                            WHEN NOT MATCHED THEN INSERT ({insert_list}) VALUES ({values_list});
                        """)
                    count = con.execute(f"SELECT COUNT(*) FROM {qi(table_name)}").fetchone()[0]
                    print(f"✅ {table_name} upsert realizado (chaves: {keys}) — total atual: {count:,}")

                else:
                    # Sem chaves → inserir apenas linhas novas por conteúdo (EXCEPT)
                    # Observação: se um registro mudou algum campo, ele será considerado "novo" e inserido.
                    proj = ", ".join(qi(c) for c in common_cols)
                    con.execute(f"""
                        INSERT INTO {qi(table_name)} ({proj})
                        SELECT {proj} FROM {qi(staging)}
                        EXCEPT
                        SELECT {proj} FROM {qi(table_name)};
                    """)
                    count = con.execute(f"SELECT COUNT(*) FROM {qi(table_name)}").fetchone()[0]
                    print(f"✅ {table_name} incremental (EXCEPT) aplicado — total atual: {count:,}")

            con.close()
            print("🎉 Carga concluída com sucesso (modo incremental).")

    finally:
        # Limpeza de temporários
        try:
            shutil.rmtree(workdir, ignore_errors=True)
        except Exception:
            pass


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
        FROM (
            SELECT *
            FROM cno
            WHERE TRY_CAST("Data de registro" AS DATE) >= DATE '2020-01-01'
        ) c
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


def main():
    extract_and_load_raw()
    transform_data()

if __name__ == "__main__":
    main()
