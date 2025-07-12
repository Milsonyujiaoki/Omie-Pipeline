# compactador_resultado_v8.py
# Requer: Python 3.9+
# Dependências: zipfile, shutil, configparser, concurrent.futures, logging, os, pathlib, datetime

import os
import logging
import zipfile
import shutil
from pathlib import Path
from datetime import datetime
import configparser
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.upload_onedrive import (
    autenticar_ms_graph,
    enviar_zip_onedrive,
    extrair_mes_do_path,
    garantir_pasta_mes
)

def carregar_limite_do_ini(config_path: str = 'configuracao.ini') -> int:
    """
    Carrega o número máximo de arquivos XML por pasta antes da compactação.

    Args:
        config_path: Caminho do arquivo de configuração INI.

    Returns:
        Limite de arquivos por pasta como inteiro. Valor padrão = 10.000.
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    return int(config.get('compactador', 'arquivos_por_pasta', fallback='10000'))

def carregar_config_onedrive(config_path: str = 'configuracao.ini') -> tuple[bool, str]:
    """
    Lê o arquivo INI e retorna se o upload está ativado e qual a pasta de destino.

    Returns:
        Tuple contendo: (upload_ativado, caminho_pasta_destino)
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    upload_onedrive = config.getboolean("ONEDRIVE", "upload_onedrive", fallback=False)
    pasta_destino = config.get("ONEDRIVE", "pasta_destino", fallback="Anexos e temporarios/notas ng/pastas completas")
    return upload_onedrive, pasta_destino

def enviar_zip_para_onedrive(zip_path: Path, token: str, pasta_destino_base: str) -> None:
    """
    Envia o arquivo ZIP para o OneDrive, garantindo que seja enviado para a subpasta correta.

    Args:
        zip_path: Caminho do arquivo .zip gerado
        token: Token de autenticação da Microsoft
        pasta_destino_base: Caminho base da pasta no OneDrive
    """
    mes_zip = extrair_mes_do_path(zip_path)
    caminho_destino_completo = f"{pasta_destino_base}/{mes_zip}"

    logging.info(f"[ONEDRIVE] Verificando/criando pasta: {caminho_destino_completo}")
    garantir_pasta_mes(token, mes_zip, pasta_destino_base)

    logging.info(f"[ONEDRIVE] Enviando arquivo {zip_path.name} para {caminho_destino_completo}")
    enviar_zip_onedrive(
        token=token,
        caminho_arquivo=zip_path,
        nome_arquivo_destino=zip_path.name,
        pasta_destino=caminho_destino_completo
    )

def processar_pasta_individual(origem: Path, arquivos_por_pasta: int, token_msgraph: str | None, pasta_destino_onedrive: str) -> None:
    """
    Processa uma única pasta de dia: organiza arquivos, cria zips e faz upload se habilitado.

    Args:
        origem: Caminho da pasta com arquivos XML.
        arquivos_por_pasta: Limite de arquivos por pasta.
        token_msgraph: Token da Microsoft Graph (se ativo).
        pasta_destino_onedrive: Caminho base remoto.
    """
    nome_raiz = origem.name
    arquivos = sorted([f for f in origem.iterdir() if f.is_file() and f.suffix.lower() == ".xml"])

    if not arquivos:
        logging.info(f"[vazio] Nenhum XML encontrado em: {origem}")
        return

    for i in range(0, len(arquivos), arquivos_por_pasta):
        indice_pasta = (i // arquivos_por_pasta) + 1
        nome_pasta = f"{nome_raiz}_pasta_{indice_pasta}"
        caminho_pasta = origem / nome_pasta
        zip_path = origem / f"{nome_pasta}.zip"

        if zip_path.exists():
            logging.info(f"[✓] ZIP já existe: {zip_path}, pulando.")
            continue

        try:
            caminho_pasta.mkdir(exist_ok=True)
        except Exception as e:
            logging.error(f"[ERRO] Falha ao criar pasta {caminho_pasta}: {e}")
            continue

        for arquivo in arquivos[i:i + arquivos_por_pasta]:
            shutil.move(str(arquivo), caminho_pasta / arquivo.name)

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(caminho_pasta):
                for file in files:
                    file_path = Path(root) / file
                    arcname = Path(nome_pasta) / file_path.relative_to(caminho_pasta)
                    zipf.write(file_path, arcname)

        logging.info(f"ZIP criado localmente: {zip_path}")

        if token_msgraph:
            try:
                enviar_zip_para_onedrive(zip_path, token_msgraph, pasta_destino_onedrive)
                logging.info(f"[✓] Upload concluído: {zip_path.name}")
            except Exception as e:
                logging.error(f"[ERRO] Falha ao enviar {zip_path.name}: {e}")

def zipar_pastas_sem_zip(resultado_dir: str) -> None:
    """
    Percorre todas as subpastas de resultado exceto a do dia atual, compacta e envia para OneDrive.

    Args:
        resultado_dir: Caminho base onde os arquivos XML estão organizados por data.
    """
    resultado_path = Path(resultado_dir)
    hoje_path = Path(datetime.today().strftime("%Y/%m/%d"))
    limite_por_zip = carregar_limite_do_ini()
    upload_onedrive, destino_onedrive = carregar_config_onedrive()
    token_msgraph = None

    if upload_onedrive:
        try:
            token_msgraph = autenticar_ms_graph()
            logging.info("[✓] Autenticado no Microsoft Graph.")
        except Exception as e:
            logging.error(f"[ERRO] Falha na autenticação OneDrive: {e}")
            token_msgraph = None

    tarefas = []
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        for ano_dir in resultado_path.iterdir():
            if not ano_dir.is_dir():
                continue
            for mes_dir in ano_dir.iterdir():
                if not mes_dir.is_dir():
                    continue
                for dia_dir in mes_dir.iterdir():
                    if not dia_dir.is_dir():
                        continue

                    caminho_data = Path(ano_dir.name) / mes_dir.name / dia_dir.name
                    if caminho_data == hoje_path:
                        logging.info(f"Ignorando pasta do dia atual: {caminho_data}")
                        continue

                    tarefas.append(executor.submit(
                        processar_pasta_individual,
                        dia_dir, limite_por_zip, token_msgraph, destino_onedrive
                    ))

        if not tarefas:
            logging.warning("Nenhuma pasta foi processada. Verifique se há dados além da data atual.")

        for futuro in as_completed(tarefas):
            try:
                futuro.result()
            except Exception as exc:
                logging.error(f"[THREAD ERRO] {exc}")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    zipar_pastas_sem_zip("C:/milson/extrator_omie_v3/resultado")
