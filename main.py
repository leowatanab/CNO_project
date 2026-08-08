"""ETL dos dados do Cadastro Nacional de Obras (CNO).

O módulo pode ser importado sem executar o pipeline. Para executar etapas
específicas, use ``python main.py --help``.
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import re
import shutil
import tempfile
import threading
import time
import unicodedata
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import date, datetime, timezone
from functools import partial
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd
import requests
from requests.adapters import HTTPAdapter


LOG = logging.getLogger("cno_etl")

# Pode ser sobrescrita sem alterar o código, por exemplo:
# CNO_URL=https://servidor/arquivo.zip python main.py extract
CNO_URL = os.getenv(
    "CNO_URL",
    "https://arquivos.receitafederal.gov.br/index.php/s/PC6732BXG9B98W3/"
    "download?path=%2F&files=cno.zip",
)
CNO_DIRECT_URL = (
    "https://arquivos.receitafederal.gov.br/public.php/dav/files/"
    "PC6732BXG9B98W3/?accept=zip&files=cno.zip"
)
BRASIL_API_URL = "https://brasilapi.com.br/api/cnpj/v1/{cnpj}"

TABLE_EMPRESA = "cnpj"
TABLE_SOCIOS = "cnpj_socios"

DOWNLOAD_CHUNK_SIZE = 8 * 1024 * 1024
COPY_CHUNK_SIZE = 8 * 1024 * 1024
DOWNLOAD_TIMEOUT = (
    float(os.getenv("CNO_CONNECT_TIMEOUT", "30")),
    float(os.getenv("CNO_READ_TIMEOUT", "300")),
)
API_TIMEOUT = (5, 20)
DOWNLOAD_ATTEMPTS = int(os.getenv("CNO_DOWNLOAD_RETRIES", "3"))

SLEEP_SECONDS = float(os.getenv("CNO_BATCH_PAUSE", "0.5"))
MAX_TENTATIVAS_PADRAO = int(os.getenv("CNO_API_RETRIES", "3"))
BATCH_SIZE = int(os.getenv("CNO_BATCH_SIZE", "50"))
MAX_WORKERS = int(os.getenv("CNO_MAX_WORKERS", "10"))
# Zero desabilita o limite. É útil em ambientes com disco restrito.
MAX_DOWNLOAD_BYTES = int(os.getenv("CNO_MAX_DOWNLOAD_BYTES", "0"))
MAX_UNCOMPRESSED_BYTES = int(os.getenv("CNO_MAX_UNCOMPRESSED_BYTES", "0"))
DUCKDB_CSV_ENCODINGS = ("utf-8", "latin-1", "utf-16")
PYTHON_TRANSCODE_ENCODINGS = ("cp1252", "latin-1")

EMPRESA_COLUMNS = [
    "cnpj",
    "razao_social",
    "nome_fantasia",
    "capital_social",
    "logradouro",
    "numero",
    "bairro",
    "municipio",
    "uf",
    "cep",
    "situacao_cadastral",
    "tipo_estabelecimento",
    "porte",
    "natureza_juridica",
    "email",
    "telefone_1",
    "telefone_2",
    "data_abertura",
    "data_consulta",
    "status_consulta",
]

SOCIOS_COLUMNS = [
    "cnpj",
    "nome_socio",
    "cnpj_cpf_do_socio",
    "qualificacao_socio",
    "data_entrada_sociedade",
    "faixa_etaria",
]

_thread_local = threading.local()


def get_md_connection():
    """Abre uma conexão com o banco ``cno`` no MotherDuck."""
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise RuntimeError("MOTHERDUCK_TOKEN não encontrado no ambiente")
    connection = duckdb.connect(f"md:cno?motherduck_token={token}")
    connection.execute("USE main")
    return connection


def qi(name: str) -> str:
    """Escapa um identificador SQL."""
    return '"' + name.replace('"', '""') + '"'


def sql_string(value: str | os.PathLike[str]) -> str:
    """Escapa um valor textual para uso em SQL gerado internamente."""
    return "'" + os.fspath(value).replace("'", "''") + "'"


def qualified_table(table_name: str) -> str:
    return f"cno.main.{qi(table_name)}"


def padronizar_cnpj(cnpj: Any) -> str:
    digits = re.sub(r"\D", "", str(cnpj))
    return digits.zfill(14)


def clean_digits(value: Any) -> str | None:
    if value is None or not str(value).strip():
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits or None


def parse_date(value: Any) -> date | None:
    if value is None or not str(value).strip():
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        LOG.warning("Data inválida recebida da API: %r", value)
        return None


def parse_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        LOG.warning("Valor numérico inválido recebido da API: %r", value)
        return 0.0


def utc_now_naive() -> datetime:
    """Timestamp UTC sem timezone, compatível com a coluna TIMESTAMP."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _download_session() -> requests.Session:
    # O retry cobre a transferência inteira em ``download_zip``. Manter outro
    # retry no adapter multiplicaria as tentativas e prenderia o CI antes que o
    # fallback pudesse ser considerado.
    adapter = HTTPAdapter(max_retries=0, pool_connections=2, pool_maxsize=2)
    session = requests.Session()
    session.headers["User-Agent"] = "CNO-ETL/2.0"
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _api_session() -> requests.Session:
    """Mantém um pool HTTP independente por worker."""
    session = getattr(_thread_local, "api_session", None)
    if session is None:
        session = requests.Session()
        session.headers["User-Agent"] = "CNO-ETL/2.0"
        adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1, max_retries=0)
        session.mount("https://", adapter)
        _thread_local.api_session = session
    return session


def _download_zip_once(url: str, destination: Path) -> int:
    partial_path = destination.with_suffix(destination.suffix + ".part")
    downloaded = 0
    last_report = time.monotonic()

    try:
        with _download_session() as session:
            with session.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT) as response:
                response.raise_for_status()
                expected = int(response.headers.get("Content-Length", "0") or 0)
                if MAX_DOWNLOAD_BYTES and expected > MAX_DOWNLOAD_BYTES:
                    raise ValueError(
                        f"Download anunciado ({expected} bytes) excede "
                        f"CNO_MAX_DOWNLOAD_BYTES ({MAX_DOWNLOAD_BYTES} bytes)"
                    )

                with partial_path.open("wb") as output:
                    for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                        if not chunk:
                            continue
                        downloaded += len(chunk)
                        if MAX_DOWNLOAD_BYTES and downloaded > MAX_DOWNLOAD_BYTES:
                            raise ValueError(
                                "Download excedeu CNO_MAX_DOWNLOAD_BYTES durante a transferência"
                            )
                        output.write(chunk)
                        if time.monotonic() - last_report >= 10:
                            if expected:
                                LOG.info(
                                    "Download: %.1f/%.1f MiB (%.1f%%)",
                                    downloaded / 2**20,
                                    expected / 2**20,
                                    downloaded * 100 / expected,
                                )
                            else:
                                LOG.info("Download: %.1f MiB", downloaded / 2**20)
                            last_report = time.monotonic()

                if expected and downloaded != expected:
                    raise IOError(
                        f"Download incompleto: recebido={downloaded}, esperado={expected}"
                    )

        if not zipfile.is_zipfile(partial_path):
            raise zipfile.BadZipFile("A resposta recebida não é um arquivo ZIP válido")
        os.replace(partial_path, destination)
        return downloaded
    except Exception:
        partial_path.unlink(missing_ok=True)
        raise


def _download_urls(primary_url: str) -> list[str]:
    configured = os.getenv("CNO_FALLBACK_URLS", "")
    fallbacks = [url.strip() for url in configured.split("|") if url.strip()]
    if primary_url != CNO_DIRECT_URL:
        fallbacks.append(CNO_DIRECT_URL)
    return list(dict.fromkeys([primary_url, *fallbacks]))


def download_zip(url: str, destination: Path) -> int:
    """Baixa em streaming e repete transferências interrompidas do início."""
    if DOWNLOAD_ATTEMPTS < 1:
        raise ValueError("CNO_DOWNLOAD_RETRIES deve ser maior que zero")

    urls = _download_urls(url)
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        selected_url = urls[(attempt - 1) % len(urls)]
        try:
            LOG.info(
                "Tentativa de download %d/%d usando %s",
                attempt,
                DOWNLOAD_ATTEMPTS,
                selected_url,
            )
            return _download_zip_once(selected_url, destination)
        except (requests.RequestException, OSError, zipfile.BadZipFile) as exc:
            if attempt == DOWNLOAD_ATTEMPTS:
                raise
            delay = min(2 ** (attempt - 1), 10)
            LOG.warning(
                "Download falhou (%d/%d): %s; nova tentativa em %ds",
                attempt,
                DOWNLOAD_ATTEMPTS,
                exc,
                delay,
            )
            time.sleep(delay)

    raise AssertionError("fluxo de retry do download terminou inesperadamente")


def raw_tables_available() -> bool:
    """Confirma que há uma carga anterior utilizável antes de aceitar fallback."""
    try:
        with closing(get_md_connection()) as connection:
            connection.execute("SELECT 1 FROM cno LIMIT 1")
            connection.execute("SELECT 1 FROM cno_areas LIMIT 1")
        return True
    except Exception as exc:
        LOG.error("Carga bruta anterior não está disponível: %s", exc)
        return False


def table_name_from_member(member_name: str) -> str:
    """Converte o nome de uma entrada do ZIP em identificador previsível."""
    stem = Path(member_name.replace("\\", "/")).stem
    stem = unicodedata.normalize("NFKD", stem).encode("ascii", "ignore").decode()
    table = re.sub(r"[^a-zA-Z0-9_]+", "_", stem).strip("_").lower()
    if not table:
        raise ValueError(f"Nome de CSV inválido no ZIP: {member_name!r}")
    if table[0].isdigit():
        table = "cno_" + table
    return table


def csv_members(archive: zipfile.ZipFile) -> list[tuple[zipfile.ZipInfo, str]]:
    members: list[tuple[zipfile.ZipInfo, str]] = []
    seen_tables: set[str] = set()
    total_size = 0

    for info in archive.infolist():
        if info.is_dir() or not info.filename.lower().endswith(".csv"):
            continue
        if info.flag_bits & 0x1:
            raise ValueError(f"CSV criptografado não é suportado: {info.filename}")
        table = table_name_from_member(info.filename)
        if table in seen_tables:
            raise ValueError(f"Mais de um CSV produziria a tabela {table!r}")
        seen_tables.add(table)
        total_size += info.file_size
        members.append((info, table))

    if not members:
        raise ValueError("O ZIP não contém arquivos CSV")
    if MAX_UNCOMPRESSED_BYTES and total_size > MAX_UNCOMPRESSED_BYTES:
        raise ValueError(
            f"Conteúdo descompactado ({total_size} bytes) excede "
            f"CNO_MAX_UNCOMPRESSED_BYTES ({MAX_UNCOMPRESSED_BYTES} bytes)"
        )
    return members


def extract_member(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    destination: Path,
) -> None:
    """Extrai uma entrada sem construir o CSV inteiro em memória."""
    with archive.open(info, "r") as source, destination.open("wb") as output:
        shutil.copyfileobj(source, output, length=COPY_CHUNK_SIZE)
    if destination.stat().st_size != info.file_size:
        raise IOError(f"Tamanho incorreto após extrair {info.filename}")


def detect_csv_encoding(path: Path, sample_size: int = 1024 * 1024) -> str:
    """Detecta os encodings nativos do leitor CSV do DuckDB."""
    with path.open("rb") as source:
        sample = source.read(sample_size)
    if sample.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    try:
        sample.decode("utf-8-sig")
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def csv_encoding_candidates(preferred: str) -> list[str]:
    candidates = [preferred, *DUCKDB_CSV_ENCODINGS]
    return list(dict.fromkeys(candidates))


def is_csv_encoding_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "encoded" in message or "invalid unicode" in message


def transcode_csv_to_utf8(
    source_path: Path,
    destination_path: Path,
    source_encoding: str,
) -> None:
    """Recodifica em streaming para UTF-8 quando o leitor nativo falha."""
    with (
        source_path.open(
            "r",
            encoding=source_encoding,
            errors="replace",
            newline="",
        ) as source,
        destination_path.open("w", encoding="utf-8", newline="") as destination,
    ):
        shutil.copyfileobj(source, destination, length=COPY_CHUNK_SIZE)


def _load_csv_to_staging(con, path: Path, staging_table: str, encoding: str) -> None:
    def load(load_path: Path, selected_encoding: str) -> None:
        con.execute(
            f"CREATE TABLE {qi(staging_table)} AS "
            f"SELECT * FROM read_csv_auto({sql_string(load_path)}, "
            f"all_varchar=true, encoding={sql_string(selected_encoding)})"
        )

    failures: list[str] = []
    for selected_encoding in csv_encoding_candidates(encoding):
        try:
            load(path, selected_encoding)
            return
        except Exception as exc:
            con.execute(f"DROP TABLE IF EXISTS {qi(staging_table)}")
            failures.append(f"{selected_encoding}: {exc}")
            if not is_csv_encoding_error(exc):
                raise
            LOG.warning(
                "%s nao carregou como %s; tentando proximo encoding",
                path.name,
                selected_encoding,
            )

    recoded_path = path.with_suffix(path.suffix + ".utf8")
    try:
        for source_encoding in PYTHON_TRANSCODE_ENCODINGS:
            try:
                LOG.warning(
                    "Recodificando %s de %s para UTF-8 em streaming",
                    path.name,
                    source_encoding,
                )
                transcode_csv_to_utf8(path, recoded_path, source_encoding)
                load(recoded_path, "utf-8")
                return
            except Exception as exc:
                con.execute(f"DROP TABLE IF EXISTS {qi(staging_table)}")
                failures.append(f"{source_encoding}->utf-8: {exc}")
                if not is_csv_encoding_error(exc):
                    raise
    finally:
        recoded_path.unlink(missing_ok=True)

    details = "\n".join(failures[-5:])
    raise RuntimeError(
        f"Nao foi possivel carregar {path.name} com os encodings testados:\n{details}"
    )


def extract_and_load_raw(
    threads: int = 8,
    url: str = CNO_URL,
    allow_stale_raw: bool = False,
) -> bool:
    """Baixa o ZIP, extrai CSVs em streaming e troca as tabelas atomicamente."""
    if threads < 1:
        raise ValueError("threads deve ser maior que zero")

    staging_tables: list[tuple[str, str]] = []
    connection = None
    with tempfile.TemporaryDirectory(prefix="cno_") as temporary_directory:
        workdir = Path(temporary_directory)
        zip_path = workdir / "cno.zip"

        LOG.info("Baixando dados do CNO")
        try:
            downloaded = download_zip(url, zip_path)
        except (requests.RequestException, OSError, zipfile.BadZipFile) as exc:
            if not allow_stale_raw or not raw_tables_available():
                raise
            LOG.warning(
                "Fonte do CNO indisponível após %d tentativa(s): %s. "
                "Mantendo a carga bruta anterior e continuando o pipeline.",
                DOWNLOAD_ATTEMPTS,
                exc,
            )
            return False
        LOG.info("ZIP recebido: %.1f MiB", downloaded / 2**20)

        try:
            connection = get_md_connection()
            connection.execute(f"PRAGMA threads={int(threads)}")

            with zipfile.ZipFile(zip_path) as archive:
                members = csv_members(archive)
                for index, (info, table) in enumerate(members, start=1):
                    csv_path = workdir / f"{index:03d}_{table}.csv"
                    staging = f"__cno_load_{table}_{uuid.uuid4().hex[:10]}"
                    LOG.info(
                        "Extraindo %s (%.1f MiB) -> %s",
                        info.filename,
                        info.file_size / 2**20,
                        table,
                    )
                    extract_member(archive, info, csv_path)
                    encoding = detect_csv_encoding(csv_path)
                    LOG.info("Carregando %s com encoding %s", table, encoding)
                    _load_csv_to_staging(connection, csv_path, staging, encoding)
                    staging_tables.append((staging, table))
                    csv_path.unlink()

            # A troca só acontece depois que todos os CSVs foram lidos. DDL do
            # DuckDB é transacional, portanto uma falha preserva as tabelas atuais.
            connection.execute("BEGIN TRANSACTION")
            try:
                for staging, table in staging_tables:
                    connection.execute(f"DROP TABLE IF EXISTS {qi(table)}")
                    connection.execute(
                        f"ALTER TABLE {qi(staging)} RENAME TO {qi(table)}"
                    )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

            staging_tables.clear()
            LOG.info("Carga bruta do CNO concluída (%d tabela(s))", len(members))
            return True
        finally:
            if connection is not None:
                for staging, _ in staging_tables:
                    try:
                        connection.execute(f"DROP TABLE IF EXISTS {qi(staging)}")
                    except Exception:
                        LOG.exception("Não foi possível remover staging %s", staging)
                connection.close()


def transform_data() -> None:
    with closing(get_md_connection()) as con:
        con.execute("""
            CREATE OR REPLACE TABLE base_cno AS
            SELECT c.*, a.* EXCLUDE (CNO, rn)
            FROM cno c
            LEFT JOIN (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY CNO ORDER BY rowid DESC) rn
                FROM cno_areas
            ) a ON c.CNO = a.CNO AND a.rn = 1
            WHERE TRY_CAST(c."Data de registro" AS DATE) >= DATE '2020-01-01'
        """)
    LOG.info("base_cno criada/atualizada com registros desde 2020")


def _backoff_seconds(attempt: int, retry_after: str | None) -> float:
    if retry_after and retry_after.isdigit():
        return min(float(retry_after), 60.0)
    return min(2 ** (attempt - 1) + random.uniform(0, 0.5), 30.0)


def get_cnpj_info(cnpj: str, max_retries: int) -> dict[str, Any]:
    """Consulta a BrasilAPI com backoff e tratamento explícito dos status HTTP."""
    if max_retries < 1:
        raise ValueError("max_retries deve ser maior que zero")

    for attempt in range(1, max_retries + 1):
        try:
            response = _api_session().get(
                BRASIL_API_URL.format(cnpj=cnpj), timeout=API_TIMEOUT
            )
            if response.status_code == 200:
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("Resposta JSON não é um objeto")
                return payload
            if response.status_code == 404:
                return {"_status": "nao_encontrado"}
            if response.status_code not in (429, 500, 502, 503, 504):
                LOG.warning("BrasilAPI retornou HTTP %s para %s", response.status_code, cnpj)
                return {"_status": "erro"}
            retry_after = response.headers.get("Retry-After")
        except (requests.RequestException, ValueError) as exc:
            LOG.warning(
                "Falha na consulta de %s (%d/%d): %s",
                cnpj,
                attempt,
                max_retries,
                exc,
            )
            retry_after = None

        if attempt < max_retries:
            time.sleep(_backoff_seconds(attempt, retry_after))

    return {"_status": "erro"}


def criar_tabelas_destino(con) -> None:
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {qualified_table(TABLE_EMPRESA)} (
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
        CREATE TABLE IF NOT EXISTS {qualified_table(TABLE_SOCIOS)} (
            cnpj VARCHAR,
            nome_socio VARCHAR,
            cnpj_cpf_do_socio VARCHAR,
            qualificacao_socio VARCHAR,
            data_entrada_sociedade DATE,
            faixa_etaria VARCHAR
        );
    """)


def processar_cnpj(cnpj: str, max_retries: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    cnpj = padronizar_cnpj(cnpj)
    info = get_cnpj_info(cnpj, max_retries)
    consulted_at = utc_now_naive()

    if "_status" in info:
        return (
            {
                "cnpj": cnpj,
                "data_consulta": consulted_at,
                "status_consulta": info["_status"],
            },
            [],
        )

    empresa = {
        "cnpj": cnpj,
        "razao_social": info.get("razao_social"),
        "nome_fantasia": info.get("nome_fantasia"),
        "capital_social": parse_float(info.get("capital_social")),
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
        "data_abertura": parse_date(info.get("data_inicio_atividade")),
        "data_consulta": consulted_at,
        "status_consulta": "ok",
    }

    socios = [
        {
            "cnpj": cnpj,
            "nome_socio": socio.get("nome_socio"),
            "cnpj_cpf_do_socio": socio.get("cnpj_cpf_do_socio"),
            "qualificacao_socio": socio.get("qualificacao_socio"),
            "data_entrada_sociedade": parse_date(socio.get("data_entrada_sociedade")),
            "faixa_etaria": socio.get("faixa_etaria"),
        }
        for socio in (info.get("qsa") or [])
        if isinstance(socio, dict)
    ]
    return empresa, socios


def _normalize_frame(table_name: str, frame: pd.DataFrame) -> pd.DataFrame:
    columns = EMPRESA_COLUMNS if table_name == TABLE_EMPRESA else SOCIOS_COLUMNS
    normalized = frame.reindex(columns=columns).copy()

    date_columns = {"data_abertura", "data_consulta", "data_entrada_sociedade"}
    for column in normalized.columns:
        if column in date_columns:
            normalized[column] = pd.to_datetime(normalized[column], errors="coerce")
        elif column == "capital_social":
            normalized[column] = pd.to_numeric(
                normalized[column], errors="coerce"
            ).fillna(0.0)
        else:
            normalized[column] = normalized[column].astype("string")
    return normalized


def normalize_and_load(con, table_name: str, frame: pd.DataFrame, mode: str = "IGNORE") -> None:
    """Normaliza e insere explicitamente as colunas esperadas."""
    if frame.empty:
        return
    if table_name not in (TABLE_EMPRESA, TABLE_SOCIOS):
        raise ValueError(f"Tabela de destino não permitida: {table_name}")
    if mode not in ("IGNORE", "REPLACE"):
        raise ValueError(f"Modo de inserção inválido: {mode}")

    normalized = _normalize_frame(table_name, frame)
    if table_name == TABLE_SOCIOS:
        normalized = normalized.drop_duplicates()
    temp_name = f"tmp_{table_name}_{uuid.uuid4().hex}"
    columns = EMPRESA_COLUMNS if table_name == TABLE_EMPRESA else SOCIOS_COLUMNS
    column_sql = ", ".join(qi(column) for column in columns)
    insert = "INSERT"
    if table_name == TABLE_EMPRESA:
        insert += " OR IGNORE" if mode == "IGNORE" else " OR REPLACE"

    con.register(temp_name, normalized)
    try:
        con.execute(
            f"{insert} INTO {qualified_table(table_name)} ({column_sql}) "
            f"SELECT {column_sql} FROM {qi(temp_name)}"
        )
    finally:
        con.unregister(temp_name)


def _batches(items: list[str], size: int) -> Iterable[list[str]]:
    if size < 1:
        raise ValueError("BATCH_SIZE deve ser maior que zero")
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _save_results(con, results, company_mode: str, replace_partners: bool) -> None:
    companies = [result[0] for result in results]
    partners = [partner for result in results for partner in result[1]]
    successful_cnpjs = [
        company["cnpj"]
        for company in companies
        if company.get("status_consulta") == "ok"
    ]

    con.execute("BEGIN TRANSACTION")
    try:
        normalize_and_load(con, TABLE_EMPRESA, pd.DataFrame(companies), company_mode)

        if replace_partners and successful_cnpjs:
            ids_name = f"tmp_ids_{uuid.uuid4().hex}"
            con.register(ids_name, pd.DataFrame({"cnpj": successful_cnpjs}))
            try:
                con.execute(
                    f"DELETE FROM {qualified_table(TABLE_SOCIOS)} AS target "
                    f"USING {qi(ids_name)} AS ids WHERE target.cnpj = ids.cnpj"
                )
            finally:
                con.unregister(ids_name)

        if partners:
            normalize_and_load(con, TABLE_SOCIOS, pd.DataFrame(partners))
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def _process_batches(
    con,
    cnpjs: list[str],
    max_retries: int,
    company_mode: str,
    replace_partners: bool,
    label: str,
) -> None:
    if MAX_WORKERS < 1:
        raise ValueError("CNO_MAX_WORKERS deve ser maior que zero")
    total = len(cnpjs)
    processed = 0
    worker = partial(processar_cnpj, max_retries=max_retries)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="cnpj") as executor:
        for batch in _batches(cnpjs, BATCH_SIZE):
            results = list(executor.map(worker, batch))
            _save_results(con, results, company_mode, replace_partners)
            processed += len(batch)
            LOG.info("%s: %d/%d", label, processed, total)
            if processed < total and SLEEP_SECONDS > 0:
                time.sleep(SLEEP_SECONDS)


def dados_cnpj() -> None:
    """Consulta e insere CNPJs ainda ausentes do banco."""
    with closing(get_md_connection()) as con:
        criar_tabelas_destino(con)
        frame = con.execute(f"""
            SELECT DISTINCT regexp_replace(base."NI do responsável", '\\D', '', 'g') cnpj
            FROM cno.base_cno AS base
            WHERE base."NI do responsável" IS NOT NULL
              AND length(regexp_replace(base."NI do responsável", '\\D', '', 'g')) = 14
              AND NOT EXISTS (
                  SELECT 1
                  FROM {qualified_table(TABLE_EMPRESA)} AS empresa
                  WHERE empresa.cnpj = regexp_replace(
                      base."NI do responsável", '\\D', '', 'g'
                  )
              )
        """).df()
        cnpjs = frame["cnpj"].dropna().astype(str).tolist()
        LOG.info("%d CNPJ(s) novo(s) para processar", len(cnpjs))
        if cnpjs:
            _process_batches(
                con,
                cnpjs,
                MAX_TENTATIVAS_PADRAO,
                "IGNORE",
                False,
                "Novos CNPJs",
            )


def reprocessar_erros() -> None:
    """Tenta novamente os CNPJs cujo último status foi ``erro``."""
    with closing(get_md_connection()) as con:
        criar_tabelas_destino(con)
        frame = con.execute(
            f"SELECT cnpj FROM {qualified_table(TABLE_EMPRESA)} "
            "WHERE status_consulta = 'erro'"
        ).df()
        cnpjs = frame["cnpj"].dropna().astype(str).tolist()
        if not cnpjs:
            LOG.info("Nenhum CNPJ com status 'erro' para reprocessar")
            return

        LOG.info("Reprocessando %d erro(s)", len(cnpjs))
        _process_batches(con, cnpjs, 10, "REPLACE", True, "Reprocessamento")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ETL do Cadastro Nacional de Obras")
    parser.add_argument(
        "stage",
        nargs="?",
        choices=("all", "extract", "transform", "cnpj", "retry"),
        default="all",
        help="etapa a executar (padrão: all)",
    )
    parser.add_argument("--threads", type=int, default=8, help="threads do DuckDB")
    parser.add_argument("--url", default=CNO_URL, help="URL do ZIP do CNO")
    parser.add_argument(
        "--allow-stale-raw",
        action="store_true",
        help=(
            "continua com as tabelas brutas anteriores quando a fonte está "
            "temporariamente indisponível"
        ),
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default=os.getenv("CNO_LOG_LEVEL", "INFO").upper(),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.stage in ("all", "extract"):
        extract_and_load_raw(
            threads=args.threads,
            url=args.url,
            allow_stale_raw=args.allow_stale_raw,
        )
    if args.stage in ("all", "transform"):
        transform_data()
    if args.stage in ("all", "cnpj"):
        dados_cnpj()
    if args.stage in ("all", "retry"):
        reprocessar_erros()


if __name__ == "__main__":
    main()
