# extrator_async.py

import os
import html
import sqlite3
import asyncio
import logging
import time
from pathlib import Path
from datetime import datetime
from typing import Any
from threading import Lock

import aiohttp

from .utils import (
    atualizar_status_xml,
    iniciar_db,
    salvar_varias_notas,
    gerar_xml_path
)
from .omie_client_async import OmieClient, carregar_configuracoes

TABLE_NAME = 'notas'

# === Controle de limite de requisições (4 por segundo) ===
ULTIMA_REQUISICAO = 0.0
LOCK = Lock()


def respeitar_limite_requisicoes():
    global ULTIMA_REQUISICAO
    with LOCK:
        agora = time.monotonic()
        tempo_decorrido = agora - ULTIMA_REQUISICAO
        if tempo_decorrido < 0.25:
            time.sleep(0.25 - tempo_decorrido)
        ULTIMA_REQUISICAO = time.monotonic()


def normalizar_nota(nf: dict[str, Any]) -> dict[str, Any]:
    try:
        return {
            'cChaveNFe': nf['compl'].get('cChaveNFe'),
            'nIdNF': nf['compl'].get('nIdNF'),
            'nIdPedido': nf['compl'].get('nIdPedido'),
            'dCan': nf['ide'].get('dCan'),
            'dEmi': nf['ide'].get('dEmi'),
            'dInut': nf['ide'].get('dInut'),
            'dReg': nf['ide'].get('dReg'),
            'dSaiEnt': nf['ide'].get('dSaiEnt'),
            'hEmi': nf['ide'].get('hEmi'),
            'hSaiEnt': nf['ide'].get('hSaiEnt'),
            'mod': nf['ide'].get('mod'),
            'nNF': nf['ide'].get('nNF'),
            'serie': nf['ide'].get('serie'),
            'tpAmb': nf['ide'].get('tpAmb'),
            'tpNF': nf['ide'].get('tpNF'),
            'cnpj_cpf': nf['nfDestInt'].get('cnpj_cpf'),
            'cRazao': nf['nfDestInt'].get('cRazao'),
            'vNF': float(nf['total']['ICMSTot'].get('vNF') or 0)
        }
    except Exception as e:
        logging.warning(f"[NORMALIZAR] Falha ao normalizar nota: {e}")
        return {}


async def call_api_com_retentativa(client, session, metodo, payload):
    max_retentativas = 5
    for tentativa in range(1, max_retentativas + 1):
        try:
            respeitar_limite_requisicoes()
            return await client.call_api(session, metodo, payload)

        except aiohttp.ClientResponseError as e:
            if e.status == 429:
                tempo_espera = 2 ** tentativa
                logging.warning(f"[RETRY] 429 - Esperando {tempo_espera}s (tentativa {tentativa})")
                await asyncio.sleep(tempo_espera)
            elif e.status >= 500:
                tempo_espera = 1 + tentativa
                logging.warning(f"[RETRY] {e.status} - Erro servidor. Tentativa {tentativa}")
                await asyncio.sleep(tempo_espera)
            else:
                logging.error(f"[API] Falha irreversível: {e}")
                raise

        except Exception as e:
            logging.error(f"[API] Erro inesperado: {e}")
            raise

    raise RuntimeError(f"[API] Falha após {max_retentativas} tentativas para {metodo}")


async def listar_nfs(client: OmieClient, config: dict[str, Any], db_name: str):
    logging.info("[NFS] Iniciando listagem de notas fiscais...")
    pagina = 1
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as session:
        while True:
            payload = {
                'pagina': pagina,
                'registros_por_pagina': config["records_per_page"],
                'apenas_importado_api': 'N',
                'dEmiInicial': config["start_date"],
                'dEmiFinal': config["end_date"],
                'tpNF': 1,
                'tpAmb': 1,
                'cDetalhesPedido': 'N',
                'cApenasResumo': 'S',
                'ordenar_por': 'CODIGO',
            }
            try:
                data = await call_api_com_retentativa(client, session, "ListarNF", payload)
                notas = data.get("nfCadastro", [])
                if not notas:
                    logging.info(f"[NFS] Página {pagina} sem notas.")
                    break

                registros = [r for nf in notas if (r := normalizar_nota(nf))]
                salvar_varias_notas(registros, db_name)

                total_paginas = data.get("total_de_paginas", 1)
                logging.info(f"[NFS] Página {pagina}/{total_paginas} processada.")
                if pagina >= total_paginas:
                    break
                pagina += 1

            except Exception as e:
                logging.exception(f"[NFS] Erro na listagem página {pagina}: {e}")
                break
    logging.info("[NFS] Listagem concluída.")


async def baixar_xml_individual(session, client, row, semaphore, db_name):
    async with semaphore:
        nIdNF, chave, data_emissao, num_nfe = row
        try:
            pasta, caminho = gerar_xml_path(chave, data_emissao, num_nfe)
            pasta.mkdir(parents=True, exist_ok=True)
            rebaixado = caminho.exists()

            data = await call_api_com_retentativa(client, session, "ObterNfe", {"nIdNfe": nIdNF})
            xml_str = html.unescape(data.get("cXmlNfe", ""))

            caminho.write_text(xml_str, encoding='utf-8')
            atualizar_status_xml(db_name, chave, caminho, xml_str, rebaixado)
            logging.info(f"[XML] XML salvo: {chave}")
        except Exception as e:
            logging.error(f"[XML] Falha ao baixar XML {chave}: {e}")


async def baixar_xmls(client: OmieClient, db_name: str):
    logging.info("[XML] Iniciando download de XMLs pendentes...")
    with sqlite3.connect(db_name) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_baixado ON notas (xml_baixado)")
        rows = conn.execute(
            f"SELECT nIdNF, cChaveNFe, dEmi, nNF FROM {TABLE_NAME} WHERE xml_baixado = 0"
        ).fetchall()

    semaphore = asyncio.Semaphore(client.semaphore._value)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        await asyncio.gather(*[
            baixar_xml_individual(session, client, row, semaphore, db_name)
            for row in rows
        ])
    logging.info("[XML] Concluído download de XMLs.")


async def main():
    logging.info("[MAIN] Início da execução assíncrona")
    config = carregar_configuracoes()
    db_name = config.get("db_name", "omie.db")

    client = OmieClient(
        app_key=config["app_key"],
        app_secret=config["app_secret"],
        calls_per_second=config["calls_per_second"],
        base_url_nf=config["base_url_nf"],
        base_url_xml=config["base_url_xml"]
    )

    iniciar_db(db_name, TABLE_NAME)
    await listar_nfs(client, config, db_name)
    await baixar_xmls(client, db_name)
    logging.info("[MAIN] Processo finalizado com sucesso")


if __name__ == "__main__":
    asyncio.run(main())
