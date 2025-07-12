# report_arquivos_vazios_v3_.py

"""
Criado por: iqueiroz@corpservices.com.br

Objetivo:
    Identificar arquivos com tamanho 0 bytes ou conteúdo textual vazio em um diretório.
    Gera um relatório Excel contendo os arquivos problemáticos para fins de auditoria, limpeza ou reprocessamento.

Dependências:
    - pandas
"""

import os
import pandas as pd
from datetime import datetime
import logging
from typing import Optional


def is_text_file_empty(filepath: str) -> bool:
    """
    Verifica se o conteúdo do arquivo é vazio (após remover espaços em branco).

    Args:
        filepath: Caminho absoluto do arquivo.

    Returns:
        True se o conteúdo for vazio, False caso contrário.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read().strip() == ""
    except Exception:
        # Arquivo binário ou ilegível como texto não é tratado como "vazio"
        return False


def verificar_arquivo(filepath: str) -> Optional[dict]:
    """
    Verifica se um arquivo possui problema de conteúdo (0 bytes ou texto vazio).

    Args:
        filepath: Caminho completo do arquivo.

    Returns:
        Um dicionário com informações do arquivo problemático, ou None se estiver ok.
    """
    try:
        size = os.path.getsize(filepath)
        last_modified = datetime.fromtimestamp(os.path.getmtime(filepath))
        file_ext = os.path.splitext(filepath)[1].lower()

        # Caso o tamanho seja 0 KB
        if size == 0:
            return {
                "Path": filepath,
                "Size (bytes)": size,
                "Issue": "0 KB",
                "Last Modified": last_modified,
                "Extension": file_ext
            }

        # Caso o arquivo seja um texto vazio
        elif is_text_file_empty(filepath):
            return {
                "Path": filepath,
                "Size (bytes)": size,
                "Issue": "Empty content",
                "Last Modified": last_modified,
                "Extension": file_ext
            }

    except Exception as e:
        logging.warning(f"Erro ao ler {filepath}: {e}")

    return None


def encontrar_arquivos_vazios_ou_zero(root_dir: str) -> list[dict]:
    """
    Percorre recursivamente o diretório raiz buscando arquivos com problemas.

    Args:
        root_dir: Caminho do diretório base onde será feita a varredura.

    Returns:
        Lista de dicionários contendo informações dos arquivos vazios ou inválidos.
    """
    registros = []

    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            resultado = verificar_arquivo(full_path)
            if resultado:
                registros.append(resultado)

    return registros


def gerar_relatorio(root_path: str = 'C:\\milson\\extrator_omie_v3\\resultado') -> None:
    """
    Gera relatório em Excel com todos os arquivos encontrados como vazios ou com 0 bytes.

    Args:
        root_path: Caminho do diretório onde a análise será realizada.
    """
    logging.info(f"Escaneando diretório: {root_path}")
    registros = encontrar_arquivos_vazios_ou_zero(root_path)

    if registros:
        df = pd.DataFrame(registros)
        report_name = "relatorio_arquivos_vazios.xlsx"
        df.to_excel(report_name, index=False)
        logging.info(f" Relatório gerado: {report_name}")
    else:
        logging.info(" Nenhum arquivo vazio ou com 0 KB foi encontrado.")
