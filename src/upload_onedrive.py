# upload_onedrive.py
# Requer: Python 3.9+
# Dependências: os, logging, msal, requests, dotenv, json, configparser

import os
import logging
import requests
import json
import configparser
from dotenv import load_dotenv
from pathlib import Path
from msal import ConfidentialClientApplication

# === Configuração de log ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# === Carrega variáveis de ambiente ===
load_dotenv()

# === Carrega configurações do arquivo INI ===
config = configparser.ConfigParser()
config.read('configuracao.ini')

UPLOAD_ENABLED = config.getboolean("ONEDRIVE", "upload_onedrive", fallback=False)

# === Variáveis de ambiente sensíveis ===
CLIENT_ID = os.getenv("ONEDRIVE_CLIENT_ID")
CLIENT_SECRET = os.getenv("ONEDRIVE_CLIENT_SECRET")
TENANT_ID = os.getenv("ONEDRIVE_TENANT_ID")
SHAREPOINT_SITE = os.getenv("SHAREPOINT_SITE")
SHAREPOINT_FOLDER = os.getenv("SHAREPOINT_FOLDER")
DRIVE_NAME = os.getenv("ONEDRIVE_DRIVE_NAME")

# === Verificação das variáveis obrigatórias ===
REQUIRED_VARS = [CLIENT_ID, CLIENT_SECRET, TENANT_ID, SHAREPOINT_SITE, SHAREPOINT_FOLDER, DRIVE_NAME]

if not UPLOAD_ENABLED:
    logging.warning("[ONEDRIVE] Upload desativado por configuração no INI (upload_onedrive = false).")

elif not all(REQUIRED_VARS):
    logging.warning("[ONEDRIVE] Variáveis de ambiente incompletas. Upload será desativado.")

else:
    logging.info("[ONEDRIVE] Upload habilitado e credenciais carregadas com sucesso.")


class OneDriveUploader:
    def __init__(self):
        if not UPLOAD_ENABLED or not all(REQUIRED_VARS):
            self.enabled = False
            logging.info("[ONEDRIVE] Uploader não ativado.")
            return

        self.enabled = True
        self.client_id = CLIENT_ID
        self.client_secret = CLIENT_SECRET
        self.tenant_id = TENANT_ID
        self.site = SHAREPOINT_SITE
        self.folder = SHAREPOINT_FOLDER
        self.drive = DRIVE_NAME

        self.token = self._get_token()

    def _get_token(self) -> str:
        logging.info("[ONEDRIVE] Obtendo token de autenticação...")
        authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        app = ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=authority
        )
        scope = ["https://graph.microsoft.com/.default"]
        token_response = app.acquire_token_silent(scope, account=None)

        if not token_response:
            token_response = app.acquire_token_for_client(scopes=scope)

        if "access_token" in token_response:
            logging.info("[ONEDRIVE] Token obtido com sucesso.")
            return token_response["access_token"]
        else:
            raise Exception("Erro ao obter token do OneDrive: " + str(token_response))

    def _get_upload_url(self, filename: str) -> str:
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site}/drives/{self.drive}/root:/{self.folder}/{filename}:/content"
        logging.debug(f"[ONEDRIVE] URL de upload gerada: {url}")
        return url

    def upload_file(self, local_path: Path) -> None:
        if not self.enabled:
            logging.warning(f"[ONEDRIVE] Upload ignorado: {local_path} (Uploader desabilitado)")
            return

        if not local_path.exists():
            logging.error(f"[ONEDRIVE] Arquivo não encontrado: {local_path}")
            return

        try:
            logging.info(f"[ONEDRIVE] Iniciando upload: {local_path.name}")
            headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/zip"}
            with open(local_path, "rb") as f:
                response = requests.put(
                    url=self._get_upload_url(local_path.name),
                    headers=headers,
                    data=f
                )
            if response.status_code in (200, 201):
                logging.info(f"[ONEDRIVE] Upload concluído com sucesso: {local_path.name}")
            else:
                logging.error(f"[ONEDRIVE] Falha no upload: {response.status_code} - {response.text}")
        except Exception as e:
            logging.exception(f"[ONEDRIVE] Erro durante upload de {local_path.name}: {e}")


# === Exemplo de uso ===
if __name__ == "__main__":
    uploader = OneDriveUploader()
    arquivo_zip = Path("resultado/2025-07-10_compactado.zip")
    uploader.upload_file(arquivo_zip)
