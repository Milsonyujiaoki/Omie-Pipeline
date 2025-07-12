# verificador_xmls.py
# Requer: Python 3.9+
# Dependências: sqlite3, datetime, pathlib, logging, concurrent.futures

import sqlite3
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Configurações fixas
DB_NAME = 'omie.db'
TABLE_NAME = 'notas'

def construir_caminho_xml(chave: str, data_emissao: str, num_nfe: str) -> Path | None:
    """
    Constrói o caminho esperado do arquivo XML com base na data e chave.

    Returns:
        Caminho Path ou None se erro de data.
    """
    try:
        data_dt = datetime.strptime(data_emissao, '%d/%m/%Y')
        nome_arquivo = f"{num_nfe}_{data_dt.strftime('%Y%m%d')}_{chave}.xml"
        pasta = Path("resultado") / data_dt.strftime('%Y') / data_dt.strftime('%m') / data_dt.strftime('%d')
        return pasta / nome_arquivo
    except Exception as e:
        logging.warning(f"[Data inválida] Chave: {chave}, Data: {data_emissao}, Erro: {e}")
        return None

def verificar_arquivo_no_disco(chave: str, data_emissao: str, num_nfe: str) -> str | None:
    """
    Verifica se o XML existe no disco.

    Returns:
        Chave se o XML existir no disco, senão None.
    """
    caminho = construir_caminho_xml(chave, data_emissao, num_nfe)
    if caminho and caminho.exists():
        logging.debug(f"[✓] XML encontrado: {caminho}")
        return chave
    return None

def verificar_arquivos_existentes() -> None:
    """
    Verifica no banco de dados todas as notas cujo XML ainda não foi baixado (xml_baixado = 0)
    e confirma se o arquivo XML correspondente já existe no disco.

    Atualiza o campo `xml_baixado = 1` caso o arquivo seja localizado fisicamente.
    """
    try:
        with sqlite3.connect(DB_NAME) as conn:
            c = conn.cursor()
            c.execute(f"SELECT cChaveNFe, dEmi, nNF FROM {TABLE_NAME} WHERE xml_baixado = 0")
            rows = c.fetchall()

            logging.info(f"{len(rows)} registros para verificação...")

            atualizacoes = []
            with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                futuros = [
                    executor.submit(verificar_arquivo_no_disco, chave, data_emissao, num_nfe)
                    for chave, data_emissao, num_nfe in rows
                ]

                for futuro in as_completed(futuros):
                    chave_valida = futuro.result()
                    if chave_valida:
                        atualizacoes.append((chave_valida,))

            if atualizacoes:
                c.executemany(
                    f"UPDATE {TABLE_NAME} SET xml_baixado = 1 WHERE cChaveNFe = ?",
                    atualizacoes
                )
                conn.commit()
                logging.info(f"[✓] {len(atualizacoes)} registros atualizados.")
            else:
                logging.info("Nenhum arquivo novo encontrado no disco.")

    except Exception as e:
        logging.exception(f"[ERRO] Falha ao conectar ou atualizar banco: {e}")

def verificar() -> None:
    """
    Função principal para ser chamada pelo pipeline principal ou agendador.

    Executa a verificação da existência física dos arquivos XML e registra logs
    sobre o progresso e resultado.
    """
    logging.info("Iniciando verificação de XMLs no disco...")
    verificar_arquivos_existentes()
    logging.info("Verificação finalizada.")
