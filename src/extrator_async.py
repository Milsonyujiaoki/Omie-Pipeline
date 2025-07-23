# extrator_async.py
# Requer: Python 3.9+
# Dependências: aiohttp, asyncio, html, sqlite3, logging, pathlib, datetime, typing, time, threading

from __future__ import annotations

import os
import sys
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

# Adicionar o diretório raiz ao path para importações
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils import (
    atualizar_status_xml,
    iniciar_db,
    salvar_varias_notas,
    gerar_xml_path,
)
from src.omie_client_async import OmieClient, carregar_configuracoes

# ---------------------------------------------------------------------------
# Configuracoo de logging centralizado
# ---------------------------------------------------------------------------
# logger = logging.getLogger(__name__)

# Configuração básica de logging para acompanhar execução
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extrator_async.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

TABLE_NAME = "notas"

# === Controle de limite de requisicões (4 por segundo) ===
ULTIMA_REQUISICAO = 0.0
LOCK = Lock()


def respeitar_limite_requisicoes() -> None:
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
            "cChaveNFe": nf["compl"].get("cChaveNFe"),
            "nIdNF": nf["compl"].get("nIdNF"),
            "nIdPedido": nf["compl"].get("nIdPedido"),
            "dCan": nf["ide"].get("dCan"),
            "dEmi": nf["ide"].get("dEmi"),
            "dInut": nf["ide"].get("dInut"),
            "dReg": nf["ide"].get("dReg"),
            "dSaiEnt": nf["ide"].get("dSaiEnt"),
            "hEmi": nf["ide"].get("hEmi"),
            "hSaiEnt": nf["ide"].get("hSaiEnt"),
            "mod": nf["ide"].get("mod"),
            "nNF": nf["ide"].get("nNF"),
            "serie": nf["ide"].get("serie"),
            "tpAmb": nf["ide"].get("tpAmb"),
            "tpNF": nf["ide"].get("tpNF"),
            "cnpj_cpf": nf["nfDestInt"].get("cnpj_cpf"),
            "cRazao": nf["nfDestInt"].get("cRazao"),
            "vNF": float(nf["total"]["ICMSTot"].get("vNF") or 0),
        }
    except Exception as exc:
        logger.warning("[NORMALIZAR] Falha ao normalizar nota: %s", exc)
        return {}


async def call_api_com_retentativa(
    client: OmieClient,
    session: aiohttp.ClientSession,
    metodo: str,
    payload: dict[str, Any],
):
    max_retentativas = 5
    for tentativa in range(1, max_retentativas + 1):
        try:
            respeitar_limite_requisicoes()
            return await client.call_api(session, metodo, payload)

        except aiohttp.ClientResponseError as exc:
            if exc.status == 429:
                tempo_espera = 2**tentativa
                logger.warning(
                    "[RETRY] 429 - Esperando %ss (tentativa %s)", tempo_espera, tentativa
                )
                await asyncio.sleep(tempo_espera)
            elif exc.status >= 500:
                tempo_espera = 1 + tentativa
                logger.warning(
                    "[RETRY] %s - Erro servidor. Tentativa %s", exc.status, tentativa
                )
                await asyncio.sleep(tempo_espera)
            else:
                logger.error("[API] Falha irreversivel: %s", exc)
                raise

        except Exception as exc:
            logger.error("[API] Erro inesperado: %s", exc)
            raise

    raise RuntimeError(f"[API] Falha apos {max_retentativas} tentativas para {metodo}")


async def listar_nfs(client: OmieClient, config: dict[str, Any], db_name: str):
    logger.info("[NFS] Iniciando listagem de notas fiscais...")
    
    # Verifica otimizações disponíveis para relatórios de progresso
    try:
        from src.utils import _verificar_views_e_indices_disponiveis
        db_otimizacoes = _verificar_views_e_indices_disponiveis(db_name)
        usar_views = True
    except ImportError:
        db_otimizacoes = {}
        usar_views = False
    
    pagina = 1
    total_registros_salvos = 0
    
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as session:
        while True:
            payload = {
                "pagina": pagina,
                "registros_por_pagina": config["records_per_page"],
                "apenas_importado_api": "N",
                "dEmiInicial": config["start_date"],
                "dEmiFinal": config["end_date"],
                "tpNF": 1,
                "tpAmb": 1,
                "cDetalhesPedido": "N",
                "cApenasResumo": "S",
                "ordenar_por": "CODIGO",
            }
            try:
                data = await call_api_com_retentativa(client, session, "ListarNF", payload)
                notas = data.get("nfCadastro", [])
                if not notas:
                    logger.info("[NFS] Pagina %s sem notas.", pagina)
                    break

                registros = [r for nf in notas if (r := normalizar_nota(nf))]
                resultado_salvamento = salvar_varias_notas(registros, db_name)
                total_registros_salvos += resultado_salvamento.get('salvos', len(registros))

                total_paginas = data.get("total_de_paginas", 1)
                logger.info("[NFS] Pagina %s/%s processada (%s registros).", pagina, total_paginas, len(registros))
                
                # Relatório de progresso usando views se disponível
                if usar_views and pagina % 10 == 0 and db_otimizacoes.get('vw_notas_mes_atual', False):
                    try:
                        with sqlite3.connect(db_name) as conn:
                            cursor = conn.execute("""
                                SELECT COUNT(*) FROM vw_notas_mes_atual 
                                WHERE dEmi BETWEEN ? AND ?
                            """, (config["start_date"], config["end_date"]))
                            count_periodo = cursor.fetchone()
                            if count_periodo:
                                logger.info(f"[NFS] Progresso: {count_periodo[0]} registros no período atual")
                    except Exception as e:
                        logger.debug(f"[NFS] Erro no relatório de progresso: {e}")
                
                if pagina >= total_paginas:
                    break
                pagina += 1

            except Exception as exc:
                logger.exception("[NFS] Erro na listagem pagina %s: %s", pagina, exc)
                break
    
    logger.info(f"[NFS] Listagem concluida. {total_registros_salvos} registros processados.")
    
    # Relatório final usando views se disponíveis
    if usar_views:
        try:
            with sqlite3.connect(db_name) as conn:
                if db_otimizacoes.get('vw_notas_mes_atual', False):
                    cursor = conn.execute("""
                        SELECT COUNT(*) as total_mes_atual
                        FROM vw_notas_mes_atual 
                        WHERE dEmi BETWEEN ? AND ?
                    """, (config["start_date"], config["end_date"]))
                    total_periodo = cursor.fetchone()
                    if total_periodo:
                        logger.info(f"[NFS] === RESULTADO FINAL ===")
                        logger.info(f"[NFS] Total no período: {total_periodo[0]} registros")
        except Exception as e:
            logger.debug(f"[NFS] Erro no relatório final: {e}")


async def baixar_xml_individual(
    session: aiohttp.ClientSession,
    client: OmieClient,
    row: tuple,
    semaphore: asyncio.Semaphore,
    db_name: str,
):
    async with semaphore:
        # Adaptação para suportar tanto 4 quanto 5 campos (com anomesdia da consulta otimizada)
        if len(row) == 5:
            nIdNF, chave, data_emissao, num_nfe, anomesdia = row
        else:
            nIdNF, chave, data_emissao, num_nfe = row
            anomesdia = None
            
        try:
            pasta, caminho = gerar_xml_path(chave, data_emissao, num_nfe)
            pasta.mkdir(parents=True, exist_ok=True)
            rebaixado = caminho.exists()

            data = await call_api_com_retentativa(
                client, session, "ObterNfe", {"nIdNfe": nIdNF}
            )
            xml_str = html.unescape(data.get("cXmlNfe", ""))

            caminho.write_text(xml_str, encoding="utf-8")
            atualizar_status_xml(db_name, chave, caminho, xml_str, rebaixado)
            logger.info("[XML] XML salvo: %s", caminho)
        except Exception as exc:
            logger.error("[XML] Falha ao baixar XML %s: %s", chave, exc)


async def baixar_xmls(client: OmieClient, db_name: str):
    logger.info("[XML] Iniciando download de XMLs pendentes...")
    
    # Importa função de otimização do utils.py
    try:
        from src.utils import _verificar_views_e_indices_disponiveis, SQLITE_PRAGMAS
        usar_otimizacoes_avancadas = True
    except ImportError:
        logger.warning("[XML] Funções de otimização não disponíveis, usando método padrão")
        usar_otimizacoes_avancadas = False
        SQLITE_PRAGMAS = {
            "journal_mode": "WAL",
            "synchronous": "NORMAL", 
            "temp_store": "MEMORY"
        }
    
    with sqlite3.connect(db_name) as conn:
        # Aplica todas as configurações de performance do SQLite
        for pragma, value in SQLITE_PRAGMAS.items():
            conn.execute(f"PRAGMA {pragma} = {value}")
        
        # Verifica otimizações disponíveis se função estiver disponível
        if usar_otimizacoes_avancadas:
            db_otimizacoes = _verificar_views_e_indices_disponiveis(db_name)
            views_disponiveis = sum(1 for k, v in db_otimizacoes.items() if k.startswith('vw_') and v)
            indices_disponiveis = sum(1 for k, v in db_otimizacoes.items() if k.startswith('idx_') and v)
            logger.info(f"[XML] Otimizações DB: {views_disponiveis} views, {indices_disponiveis} índices específicos")
        else:
            db_otimizacoes = {}
            # Cria índice básico se não existir
            conn.execute("CREATE INDEX IF NOT EXISTS idx_baixado ON notas (xml_baixado)")
        
        # Consulta otimizada usando views/índices hierarquicamente
        if db_otimizacoes.get('vw_notas_pendentes', False):
            # Usa view otimizada que já tem filtros e índices aplicados
            logger.debug("[XML] Usando view otimizada 'vw_notas_pendentes'")
            cursor = conn.execute("""
                SELECT nIdNF, cChaveNFe, dEmi, nNF, anomesdia
                FROM vw_notas_pendentes
                WHERE nIdNF IS NOT NULL
                ORDER BY anomesdia DESC, cChaveNFe
            """)
        elif db_otimizacoes.get('idx_anomesdia_baixado', False):
            # Usa índice específico disponível
            logger.debug("[XML] Usando índice específico 'idx_anomesdia_baixado'")
            cursor = conn.execute("""
                SELECT nIdNF, cChaveNFe, dEmi, nNF, anomesdia
                FROM notas INDEXED BY idx_anomesdia_baixado
                WHERE xml_baixado = 0 AND nIdNF IS NOT NULL
                ORDER BY anomesdia DESC, cChaveNFe
            """)
        elif db_otimizacoes.get('idx_baixado', False):
            # Usa índice genérico para xml_baixado
            logger.debug("[XML] Usando índice 'idx_baixado'")
            cursor = conn.execute("""
                SELECT nIdNF, cChaveNFe, dEmi, nNF,
                       COALESCE(anomesdia, 
                               CASE WHEN dEmi IS NOT NULL 
                                    THEN CAST(REPLACE(dEmi, '-', '') AS INTEGER)
                                    ELSE NULL END) as anomesdia
                FROM notas INDEXED BY idx_baixado
                WHERE xml_baixado = 0 AND nIdNF IS NOT NULL
                ORDER BY anomesdia DESC NULLS LAST, cChaveNFe
            """)
        else:
            # Query padrão sem hints específicos
            logger.debug("[XML] Usando consulta padrão sem índices específicos")
            cursor = conn.execute("""
                SELECT nIdNF, cChaveNFe, dEmi, nNF,
                       COALESCE(anomesdia, 
                               CASE WHEN dEmi IS NOT NULL 
                                    THEN CAST(REPLACE(dEmi, '-', '') AS INTEGER)
                                    ELSE NULL END) as anomesdia
                FROM notas 
                WHERE xml_baixado = 0 AND nIdNF IS NOT NULL
                ORDER BY anomesdia DESC NULLS LAST, cChaveNFe
            """)
        
        rows = cursor.fetchall()
        
        # Log de estatísticas usando views se disponíveis
        if db_otimizacoes.get('vw_notas_mes_atual', False):
            try:
                cursor = conn.execute("""
                    SELECT COUNT(*) as pendentes_mes_atual
                    FROM vw_notas_mes_atual 
                    WHERE xml_baixado = 0 AND nIdNF IS NOT NULL
                """)
                pendentes_mes = cursor.fetchone()
                if pendentes_mes:
                    logger.info(f"[XML] Pendentes no mês atual: {pendentes_mes[0]}")
            except Exception as e:
                logger.debug(f"[XML] Erro ao obter estatísticas do mês: {e}")
    
    total_pendentes = len(rows)
    logger.info(f"[XML] {total_pendentes} XMLs pendentes encontrados para download")
    
    if not rows:
        logger.info("[XML] Nenhum XML pendente para download")
        return

    semaphore = asyncio.Semaphore(client.semaphore._value)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        await asyncio.gather(
            *[
                baixar_xml_individual(session, client, row, semaphore, db_name)
                for row in rows
            ]
        )
    logger.info("[XML] Concluido download de XMLs.")
    
    # Relatório final usando views se disponíveis
    if usar_otimizacoes_avancadas:
        try:
            with sqlite3.connect(db_name) as conn:
                # Usa views para relatório pós-execução
                if db_otimizacoes.get('vw_notas_mes_atual', False):
                    cursor = conn.execute("""
                        SELECT 
                            COUNT(*) as total_mes,
                            SUM(CASE WHEN xml_baixado = 1 THEN 1 ELSE 0 END) as baixados_mes,
                            SUM(CASE WHEN xml_vazio = 1 THEN 1 ELSE 0 END) as vazios_mes
                        FROM vw_notas_mes_atual
                    """)
                    stats_mes = cursor.fetchone()
                    if stats_mes:
                        percentual = (stats_mes[1] / stats_mes[0] * 100) if stats_mes[0] > 0 else 0
                        logger.info(f"[XML] === ESTATÍSTICAS FINAIS (MÊS ATUAL) ===")
                        logger.info(f"[XML] Total: {stats_mes[0]} | Baixados: {stats_mes[1]} ({percentual:.1f}%) | Vazios: {stats_mes[2]}")
                
                # Estatísticas gerais sempre disponíveis
                cursor = conn.execute("""
                    SELECT 
                        COUNT(*) as total_geral,
                        SUM(CASE WHEN xml_baixado = 1 THEN 1 ELSE 0 END) as baixados_geral,
                        SUM(CASE WHEN erro = 1 THEN 1 ELSE 0 END) as erros_geral
                    FROM notas
                """)
                stats_geral = cursor.fetchone()
                if stats_geral:
                    percentual_geral = (stats_geral[1] / stats_geral[0] * 100) if stats_geral[0] > 0 else 0
                    logger.info(f"[XML] === ESTATÍSTICAS GERAIS ===")
                    logger.info(f"[XML] Total: {stats_geral[0]} | Baixados: {stats_geral[1]} ({percentual_geral:.1f}%) | Erros: {stats_geral[2]}")
                    
        except Exception as e:
            logger.debug(f"[XML] Erro ao obter estatísticas finais: {e}")


async def main():
    logger.info("[MAIN] Inicio da execucao assincrona")
    config = carregar_configuracoes()
    db_name = config.get("db_name", "omie.db")

    client = OmieClient(
        app_key=config["app_key"],
        app_secret=config["app_secret"],
        calls_per_second=config["calls_per_second"],
        base_url_nf=config["base_url_nf"],
        base_url_xml=config["base_url_xml"],
    )

    iniciar_db(db_name, TABLE_NAME)
    await listar_nfs(client, config, db_name)
    await baixar_xmls(client, db_name)
    logger.info("[MAIN] Processo finalizado com sucesso")


if __name__ == "__main__":
    asyncio.run(main())
