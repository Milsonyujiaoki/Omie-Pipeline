# upload_onedrive.py

import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import requests

load_dotenv()  # Carrega variáveis do .env

# Recupera variáveis de ambiente
CLIENT_ID = os.getenv("MS_CLIENT_ID")
CLIENT_SECRET = os.getenv("MS_CLIENT_SECRET")
TENANT_ID = os.getenv("MS_TENANT_ID")
USER_ID = os.getenv("MS_USER_ID")  # ID ou e-mail do usuário (ex: 'nome@empresa.com')


# ----------------------------
# Fallback para execução local
# ----------------------------
UPLOAD_ATIVO = all([CLIENT_ID, CLIENT_SECRET, TENANT_ID, USER_ID])
if not UPLOAD_ATIVO:
    logging.warning("[ONEDRIVE] Variáveis de ambiente não definidas. Upload será ignorado.")


# ----------------------------
def autenticar_ms_graph() -> str:
    """
    Autentica usando client_credentials e retorna token de acesso.
    """
    if not UPLOAD_ATIVO:
        raise RuntimeError("Credenciais MS não configuradas")

    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default"
    }
    response = requests.post(url, data=data)
    response.raise_for_status()
    token = response.json()["access_token"]
    return token


# ----------------------------
def extrair_mes_do_path(path: Path) -> str:
    """
    Extrai o mês no formato yyyy-mm do caminho do arquivo.
    Ex: .../2024/05/28/saida_pasta_1.zip => "2024-05"
    """
    partes = path.parts
    for i in range(len(partes)):
        if partes[i].isdigit() and len(partes[i]) == 4:  # ano
            ano = partes[i]
            if i+1 < len(partes) and partes[i+1].isdigit():
                mes = partes[i+1].zfill(2)
                return f"{ano}-{mes}"
    raise ValueError(f"Não foi possível extrair mês do path: {path}")


# ----------------------------
def garantir_pasta_mes(token: str, mes: str, base_pasta: str) -> None:
    """
    Verifica se pasta do mês existe. Se não, tenta criar.
    """
    if not UPLOAD_ATIVO:
        return

    headers = {"Authorization": f"Bearer {token}"}

    # Obtém ID da pasta base
    url_base = f"https://graph.microsoft.com/v1.0/users/{USER_ID}/drive/root:/{base_pasta}"
    r = requests.get(url_base, headers=headers)
    if r.status_code == 404:
        raise FileNotFoundError(f"[ONEDRIVE] Pasta base não encontrada: {base_pasta}")
    r.raise_for_status()
    id_base = r.json()["id"]

    # Verifica se pasta do mês já existe
    url_list = f"https://graph.microsoft.com/v1.0/users/{USER_ID}/drive/items/{id_base}/children"
    resposta = requests.get(url_list, headers=headers)
    resposta.raise_for_status()
    pastas = resposta.json().get("value", [])
    for pasta in pastas:
        if pasta["name"].lower() == mes.lower():
            return  # já existe

    # Cria pasta do mês
    url_create = f"https://graph.microsoft.com/v1.0/users/{USER_ID}/drive/items/{id_base}/children"
    payload = {
        "name": mes,
        "folder": {},
        "@microsoft.graph.conflictBehavior": "rename"
    }
    r_create = requests.post(url_create, headers=headers, json=payload)
    r_create.raise_for_status()
    logging.info(f"[ONEDRIVE] Pasta criada: {mes}")


# ----------------------------
def enviar_zip_onedrive(token: str, caminho_arquivo: Path, nome_arquivo_destino: str, pasta_destino: str) -> None:
    """
    Envia o arquivo ZIP para o destino correto no OneDrive.
    """
    if not UPLOAD_ATIVO:
        logging.warning(f"[ONEDRIVE] Upload desativado: {caminho_arquivo.name}")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream"
    }

    # Caminho completo de destino
    url = f"https://graph.microsoft.com/v1.0/users/{USER_ID}/drive/root:/{pasta_destino}/{nome_arquivo_destino}:/content"
    with open(caminho_arquivo, "rb") as f:
        r = requests.put(url, headers=headers, data=f)
    r.raise_for_status()
    logging.info(f"[ONEDRIVE] Upload concluído: {nome_arquivo_destino}")