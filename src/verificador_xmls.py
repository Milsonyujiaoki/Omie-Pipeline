# verificador_xmls.py
# Requer: Python 3.9+
# Dependências: os, sqlite3, logging, pathlib, concurrent.futures
# Finalidade: Verificar se os XMLs já existem no disco e atualizar seu status no banco de dados.

import os
import sqlite3
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from .utils import gerar_xml_path

# === Configurações globais ===
DB_PATH = "omie.db"
TABLE_NAME = "notas"
MAX_WORKERS = os.cpu_count() or 4


def verificar_arquivo_no_disco(row: tuple) -> str | None:
    """
    Verifica se o XML correspondente à nota existe no disco e possui conteúdo válido (>0 bytes).

    Args:
        row: Tupla com (cChaveNFe, dEmi, nNF).

    Returns:
        A chave da nota (cChaveNFe) se o arquivo for válido, ou None caso contrário.
    """
    chave, data_emissao, num_nfe = row

    if not chave or not data_emissao or not num_nfe:
        logging.warning(f"[IGNORADO] Campos ausentes para chave {chave}")
        return None

    try:
        _, caminho = gerar_xml_path(chave, data_emissao, num_nfe)
        if caminho.exists() and caminho.stat().st_size > 0:
            return chave
        return None
    except Exception as e:
        logging.warning(f"[ERRO] Erro ao verificar arquivo {chave}: {e}")
        return None


def verificar_arquivos_existentes(db_path: str = DB_PATH) -> list[str]:
    """
    Carrega registros do banco e verifica quais arquivos XML estão presentes e válidos no disco.

    Args:
        db_path: Caminho do banco de dados SQLite.

    Returns:
        Lista de chaves (cChaveNFe) cujos arquivos estão presentes e válidos.
    """
    logging.info(" Iniciando verificação dos arquivos XML no disco...")

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT cChaveNFe, dEmi, nNF FROM {TABLE_NAME} WHERE xml_baixado = 0"
        ).fetchall()

    logging.info(f"Total de notas pendentes no banco: {len(rows)}")

    chaves_validas = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futuros = {executor.submit(verificar_arquivo_no_disco, row): row for row in rows}
        for futuro in as_completed(futuros):
            chave = futuro.result()
            if chave:
                chaves_validas.append(chave)

    logging.info(f" Total de XMLs válidos detectados no disco: {len(chaves_validas)}")
    return chaves_validas


def atualizar_status_no_banco(chaves: list[str], db_path: str = DB_PATH) -> None:
    """
    Atualiza os registros no banco, marcando como baixado (xml_baixado = 1) para as chaves fornecidas.

    Args:
        chaves: Lista de chaves fiscais com XML existente.
        db_path: Caminho do banco SQLite.
    """
    if not chaves:
        logging.info("Nenhuma chave para atualizar no banco.")
        return

    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")

            conn.executemany(
                f"UPDATE {TABLE_NAME} SET xml_baixado = 1 WHERE cChaveNFe = ?",
                [(chave,) for chave in chaves]
            )
            conn.commit()

        logging.info(f" {len(chaves)} registros atualizados com sucesso no banco.")
    except Exception as e:
        logging.exception(f"[ERRO] Falha ao atualizar o banco de dados: {e}")


def verificar() -> None:
    """
    Função principal para orquestrar a verificação e atualização do status dos XMLs no banco de dados.
    """
    logging.info(" Iniciando verificação de XMLs salvos no disco...")
    chaves_com_arquivo = verificar_arquivos_existentes(DB_PATH)
    atualizar_status_no_banco(chaves_com_arquivo, DB_PATH)
    logging.info(" Verificação de arquivos finalizada.")


# Execução direta
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    verificar()
