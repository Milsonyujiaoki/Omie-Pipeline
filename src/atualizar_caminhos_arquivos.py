# atualizar_caminhos_arquivos_v3.py
# Requer: Python 3.9+
# Dependências: os, sqlite3, pathlib, configparser, logging, concurrent.futures

import os
import sqlite3
import logging
from pathlib import Path
from configparser import ConfigParser
from concurrent.futures import ThreadPoolExecutor, as_completed

# Constantes globais: nome do banco de dados e nome da tabela usada
DB_NAME = 'omie.db'
TABLE_NAME = 'notas'

def carregar_resultado_dir(config_path: str = 'configuracao.ini') -> Path:
    """
    Lê o caminho da pasta 'resultado' do arquivo de configuração INI.

    Args:
        config_path: Caminho do arquivo de configuração INI.

    Returns:
        Objeto Path com o diretório de resultado onde os XMLs foram salvos.
    """
    config = ConfigParser()
    config.read(config_path)
    return Path(config.get('paths', 'resultado_dir', fallback='resultado'))

def extrair_chave_arquivo(nome_arquivo: str) -> str:
    """
    Extrai a chave da NFe (cChaveNFe) do nome do arquivo XML.
    O nome do arquivo geralmente segue o padrão: YYYYMMDD_NUMERO_CHAVE.xml

    Args:
        nome_arquivo: Nome do arquivo XML.

    Returns:
        A chave da NFe extraída ou string vazia se não for possível extrair.
    """
    partes = nome_arquivo.replace('.xml', '').split('_')
    if len(partes) >= 3:
        return partes[2]
    return ""

def verificar_vazio(path: Path) -> bool:
    """
    Verifica se um arquivo XML está vazio (sem conteúdo útil).

    Args:
        path: Caminho para o arquivo XML.

    Returns:
        True se o arquivo estiver vazio ou com erro de leitura; False caso contrário.
    """
    try:
        if path.stat().st_size == 0:
            return True
        with open(path, 'r', encoding='utf-8') as f:
            return f.read().strip() == ''
    except Exception:
        return True

def processar_arquivo(path: Path) -> tuple[str, str, int, str] | None:
    """
    Processa um único arquivo XML, extraindo a chave e verificando se está vazio.

    Args:
        path: Caminho completo do arquivo XML.

    Returns:
        Tupla com (caminho, baixado, vazio, chave) ou None se inválido.
    """
    chave = extrair_chave_arquivo(path.name)
    if not chave:
        return None
    caminho_arquivo = str(path.resolve())
    xml_vazio = 1 if verificar_vazio(path) else 0
    return (caminho_arquivo, 1, xml_vazio, chave)

def atualizar_caminhos_no_banco():
    """
    Atualiza o banco SQLite com os caminhos absolutos dos arquivos XML,
    marcando se foram baixados e se estão vazios. Utiliza paralelismo para ganho de performance.
    Aplica PRAGMA WAL e cria índices para otimizar desempenho.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    resultado_dir = carregar_resultado_dir()
    arquivos_xml = list(resultado_dir.rglob('*.xml'))
    logging.info(f"{len(arquivos_xml)} arquivos encontrados em {resultado_dir}")

    atualizacoes = []
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futuros = [executor.submit(processar_arquivo, path) for path in arquivos_xml]
        for futuro in as_completed(futuros):
            resultado = futuro.result()
            if resultado:
                atualizacoes.append(resultado)

    if atualizacoes:
        with sqlite3.connect(DB_NAME) as conn:
            # PRAGMAs para performance
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA temp_store = MEMORY")

            # Índices para performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_chave ON notas(cChaveNFe)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_baixado ON notas(xml_baixado)")

            conn.executemany(f'''
                UPDATE {TABLE_NAME}
                SET caminho_arquivo = ?,
                    xml_baixado = ?,
                    xml_vazio = ?
                WHERE cChaveNFe = ?''', atualizacoes)
            conn.commit()
        logging.info(f"{len(atualizacoes)} registros atualizados no banco.")
    else:
        logging.info("Nenhum arquivo correspondeu a registros no banco.")

if __name__ == "__main__":
    atualizar_caminhos_no_banco()
