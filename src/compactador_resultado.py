# compactador_resultado.py
# Requer: Python 3.9+
# Dependências: zipfile, shutil, configparser, concurrent.futures, logging, os, pathlib, datetime

"""Módulo responsável por compactar pastas de XML em arquivos ZIP
   e, opcionalmente, enviá‑los para o OneDrive (via ``upload_onedrive.OneDriveUploader``).

   Melhorias implementadas
   -----------------------
   * Logging estruturado em início/fim de cada etapa.
   * Escaneamento paralelo por dia usando ``ThreadPoolExecutor``.
   * Validação pró‑ativa de existência de pasta/arquivo e tratamento de exceções.
   * Respeito a limite configurável de arquivos por ZIP (``configuracao.ini``).
   * Garantia de criação de subpastas por mês no OneDrive.
"""

from __future__ import annotations

import os
import logging
import zipfile
import shutil
from pathlib import Path
from datetime import datetime
import configparser
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from upload_onedrive import OneDriveUploader

# ---------------------------------------------------------------------------
# Configurações e utilidades
# ---------------------------------------------------------------------------

CONFIG_PATH = "configuracao.ini"
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

LIMITE_POR_PASTA = int(config.get("compactador", "arquivos_por_pasta", fallback="10000"))
RESULTADO_DIR = Path(config.get("paths", "resultado_dir", fallback="resultado"))
UPLOAD_ONEDRIVE = config.getboolean("ONEDRIVE", "upload_onedrive", fallback=False)
PASTA_DESTINO_ONEDRIVE = config.get("ONEDRIVE", "pasta_destino", fallback="Anexos e temporarios/notas ng/pastas completas")

# ---------------------------------------------------------------------------
# Funções internas
# ---------------------------------------------------------------------------

def _compactar_pasta(origem: Path, limite: int, uploader: Optional[OneDriveUploader]) -> None:
    """Compacta arquivos XML em lotes e envia ao OneDrive se habilitado."""
    logging.info(f"[ZIP] Processando pasta‑dia: {origem}")
    xmls = sorted([f for f in origem.iterdir() if f.is_file() and f.suffix.lower() == ".xml"])
    if not xmls:
        logging.debug(f"[ZIP] Nenhum XML encontrado em {origem}")
        return

    for i in range(0, len(xmls), limite):
        indice = (i // limite) + 1
        subfolder = origem / f"{origem.name}_pasta_{indice}"
        zip_path = origem / f"{origem.name}_pasta_{indice}.zip"

        if zip_path.exists():
            logging.info(f"[ZIP] Já existe: {zip_path}, pulando.")
            continue

        subfolder.mkdir(exist_ok=True)
        for xml_file in xmls[i:i + limite]:
            shutil.move(str(xml_file), subfolder / xml_file.name)

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(subfolder):
                for file in files:
                    file_path = Path(root) / file
                    arcname = Path(subfolder.name) / file_path.relative_to(subfolder)
                    zipf.write(file_path, arcname)
        logging.info(f"[ZIP] Criado: {zip_path}")

        # Upload opcional
        if uploader and uploader.enabled:
            try:
                uploader.upload_file(zip_path)
            except Exception as exc:
                logging.error(f"[ONEDRIVE] Falha no upload de {zip_path.name}: {exc}")


def _processar_dia(dia_dir: Path, limite: int, uploader: Optional[OneDriveUploader]) -> None:
    """Wrapper para chamada paralela por ThreadPoolExecutor."""
    try:
        _compactar_pasta(dia_dir, limite, uploader)
    except Exception as err:
        logging.exception(f"[ERRO] Exceção ao processar {dia_dir}: {err}")

# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------

def compactar_resultado() -> None:
    logging.info("[MAIN] Iniciando compactação de pastas resultado…")
    hoje = Path(datetime.today().strftime("%Y/%m/%d"))
    uploader = OneDriveUploader() if UPLOAD_ONEDRIVE else None

    tarefas = []
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        for ano in RESULTADO_DIR.iterdir():
            if not ano.is_dir():
                continue
            for mes in ano.iterdir():
                if not mes.is_dir():
                    continue
                for dia in mes.iterdir():
                    if not dia.is_dir():
                        continue
                    if Path(ano.name) / mes.name / dia.name == hoje:
                        logging.debug(f"[SKIP] Pasta do dia atual ignorada: {dia}")
                        continue
                    tarefas.append(executor.submit(_processar_dia, dia, LIMITE_POR_PASTA, uploader))

        for fut in as_completed(tarefas):
            fut.result()

    logging.info("[MAIN] Compactação finalizada.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    compactar_resultado()
