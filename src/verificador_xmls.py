"""
verificador_xmls.py

Finalidade:
    Verificar quais arquivos XML de notas fiscais ja estoo presentes no disco e
    atualizar o banco de dados marcando-os como baixados.

Requisitos:
    - Python 3.9+
    - Modulos: os, sqlite3, logging, pathlib, concurrent.futures

Autor:
    Equipe de Integracoo Omie - CorpServices
"""

import os
import sqlite3
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
from pathlib import Path

# Adiciona o diretório atual ao path para importar utils
sys.path.insert(0, str(Path(__file__).parent))

from utils import gerar_xml_path, gerar_xml_path_otimizado  # Utilitario centralizado

# ------------------------------------------------------------------------------
# Configuracões globais
# ------------------------------------------------------------------------------

DB_PATH = "omie.db"  # Caminho do banco na raiz do projeto
TABLE_NAME = "notas"
MAX_WORKERS = os.cpu_count() or 4
USE_OPTIMIZED_VERSION = True  # Flag para usar versão otimizada

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# verificacao de existência de arquivos XML
# ------------------------------------------------------------------------------

def verificar_arquivo_no_disco(row: tuple[str, str, str]) -> str | None:
    """
    Verifica se o XML correspondente à nota existe no disco e possui conteudo valido (> 0 bytes).

    Args:
        row: Tupla contendo (cChaveNFe, dEmi, nNF).

    Returns:
        A chave fiscal (cChaveNFe) se o arquivo for valido, ou None caso contrario.

    Raises:
        Exception: Se ocorrer erro inesperado ao acessar o arquivo.
    """
    chave, dEmi, num_nfe = row

    if not chave or not dEmi or not num_nfe:
        logger.warning(f"[IGNORADO] Campos ausentes para chave: {chave} | {dEmi} | {num_nfe}")
        return None

    try:
        # Usa versão otimizada ou original baseado na configuração global
        if USE_OPTIMIZED_VERSION:
            _, caminho = gerar_xml_path_otimizado(chave, dEmi, num_nfe)
        else:
            _, caminho = gerar_xml_path(chave, dEmi, num_nfe)
            
        if caminho.exists() and caminho.stat().st_size > 0:
            return chave
        return None
    except Exception as e:
        logger.warning(f"[ERRO] Falha ao verificar arquivo da chave {chave}: {e}")
        return None

# ------------------------------------------------------------------------------
# Leitura do banco de dados e paralelizacoo da verificacao
# ------------------------------------------------------------------------------

def verificar_arquivos_existentes(
    db_path: str = DB_PATH,
    max_workers: int = MAX_WORKERS,
    batch_size: int = 500
) -> list[str]:
    """
    Carrega as notas pendentes no banco e verifica quais arquivos XML ja estoo salvos e validos no disco.

    Args:
        db_path: Caminho do banco SQLite.
        max_workers: Numero de threads para paralelismo.
        batch_size: Tamanho do lote para logs de progresso.

    Returns:
        Lista de chaves fiscais (cChaveNFe) com arquivos encontrados.

    Raises:
        Exception: Se ocorrer erro inesperado durante a verificacao.
    """
    versao_funcao = "OTIMIZADA" if USE_OPTIMIZED_VERSION else "ORIGINAL"
    logger.info(f"[DISCO] Iniciando verificacao de arquivos XML no disco... (versão {versao_funcao})")
    logger.info(f"[DISCO] Configuração: workers={max_workers}, batch={batch_size}")

    with sqlite3.connect(db_path) as conn:
        rows: list[tuple[str, str, str]] = conn.execute(
            f"SELECT cChaveNFe, dEmi, nNF FROM {TABLE_NAME} WHERE xml_baixado = 0"
        ).fetchall()

    total = len(rows)
    logger.info(f"[DISCO] Total de notas pendentes no banco: {total}")

    chaves_validas: list[str] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futuros = {executor.submit(verificar_arquivo_no_disco, row): row for row in rows}
        for idx, futuro in enumerate(as_completed(futuros), 1):
            try:
                chave = futuro.result()
                if chave:
                    chaves_validas.append(chave)
                if idx % batch_size == 0 or idx == total:
                    logger.info(f"[PROGRESSO] Verificados {idx}/{total} arquivos. Validos ate agora: {len(chaves_validas)}")
            except Exception as e:
                logger.error(f"[DISCO] Erro ao processar verificacao paralela: {e}")

    taxa_sucesso = (len(chaves_validas) / total) * 100 if total > 0 else 0
    logger.info(f"[DISCO] XMLs validos encontrados: {len(chaves_validas)} ({taxa_sucesso:.2f}%)")
    logger.info(f"[DISCO] Função utilizada: {versao_funcao}")
    
    return chaves_validas

# ------------------------------------------------------------------------------
# atualizacao do status xml_baixado no banco
# ------------------------------------------------------------------------------


def atualizar_status_no_banco(
    chaves: list[str],
    db_path: str = DB_PATH,
    batch_size: int = 500
) -> None:
    """
    Atualiza o campo xml_baixado = 1 para todas as chaves com arquivos confirmados no disco.

    Args:
        chaves: Lista de chaves fiscais encontradas no disco.
        db_path: Caminho do banco SQLite.
        batch_size: Tamanho do lote para atualizacao.

    Raises:
        Exception: Se ocorrer erro inesperado durante a atualizacao.
    """
    if not chaves:
        logger.info("[BANCO] Nenhuma chave para atualizar.")
        return

    total = len(chaves)
    logger.info(f"[BANCO] Iniciando atualizacao de {total} registros em lotes de {batch_size}...")
    try:
        for i in range(0, total, batch_size):
            lote = chaves[i:i+batch_size]
            with sqlite3.connect(db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA temp_store=MEMORY")
                conn.executemany(
                    f"UPDATE {TABLE_NAME} SET xml_baixado = 1 WHERE cChaveNFe = ?",
                    [(chave,) for chave in lote]
                )
                conn.commit()
            logger.info(f"[BANCO] Atualizados {min(i+batch_size, total)}/{total} registros.")
        logger.info(f"[BANCO] {total} registros atualizados com sucesso.")
    except Exception as e:
        logger.exception(f"[BANCO] Falha ao atualizar registros: {e}")

# ------------------------------------------------------------------------------
# funcao principal de orquestracoo
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# funcao principal de orquestracoo
# ------------------------------------------------------------------------------

def comparar_funcoes_gerar_xml(
    db_path: str = DB_PATH,
    num_amostras: int = 100
) -> dict:
    """
    Compara a performance e consistência das funções gerar_xml_path.
    
    Args:
        db_path: Caminho do banco SQLite.
        num_amostras: Número de amostras para teste.
        
    Returns:
        Dicionário com resultados da comparação.
    """
    logger.info(f"[COMPARACAO] Iniciando comparação entre funções de geração de XML (amostras={num_amostras})...")
    
    import time
    
    # Busca amostras do banco
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT cChaveNFe, dEmi, nNF FROM {TABLE_NAME} WHERE xml_baixado = 0 LIMIT {num_amostras}"
        ).fetchall()
    
    if not rows:
        logger.warning("[COMPARACAO] Nenhuma amostra encontrada no banco")
        return {"erro": "Sem dados para comparação"}
    
    resultados = {
        "amostras_testadas": len(rows),
        "tempo_original": 0,
        "tempo_otimizada": 0,
        "resultados_identicos": 0,
        "resultados_diferentes": 0,
        "erros_original": 0,
        "erros_otimizada": 0,
        "casos_divergentes": []
    }
    
    # Teste função original
    logger.info("[COMPARACAO] Testando função original...")
    inicio = time.perf_counter()
    resultados_original = []
    
    for chave, dEmi, num_nfe in rows:
        try:
            _, caminho = gerar_xml_path(chave, dEmi, num_nfe)
            resultados_original.append(str(caminho))
        except Exception as e:
            resultados_original.append(f"ERRO: {e}")
            resultados["erros_original"] += 1
    
    resultados["tempo_original"] = time.perf_counter() - inicio
    
    # Teste função otimizada
    logger.info("[COMPARACAO] Testando função otimizada...")
    inicio = time.perf_counter()
    resultados_otimizada = []
    
    for chave, dEmi, num_nfe in rows:
        try:
            _, caminho = gerar_xml_path_otimizado(chave, dEmi, num_nfe)
            resultados_otimizada.append(str(caminho))
        except Exception as e:
            resultados_otimizada.append(f"ERRO: {e}")
            resultados["erros_otimizada"] += 1
    
    resultados["tempo_otimizada"] = time.perf_counter() - inicio
    
    # Comparação de resultados
    for i, (orig, otim) in enumerate(zip(resultados_original, resultados_otimizada)):
        if orig == otim:
            resultados["resultados_identicos"] += 1
        else:
            resultados["resultados_diferentes"] += 1
            if len(resultados["casos_divergentes"]) < 5:  # Limita a 5 exemplos
                resultados["casos_divergentes"].append({
                    "index": i,
                    "chave": rows[i][0][:20] + "...",
                    "original": orig,
                    "otimizada": otim
                })
    
    # Cálculo de métricas
    if resultados["tempo_original"] > 0:
        melhoria = ((resultados["tempo_original"] - resultados["tempo_otimizada"]) / resultados["tempo_original"]) * 100
        resultados["melhoria_percentual"] = melhoria
    else:
        resultados["melhoria_percentual"] = 0
    
    # Log dos resultados
    logger.info(f"[COMPARACAO] Tempo original: {resultados['tempo_original']:.4f}s")
    logger.info(f"[COMPARACAO] Tempo otimizada: {resultados['tempo_otimizada']:.4f}s")
    logger.info(f"[COMPARACAO] Melhoria: {resultados['melhoria_percentual']:+.1f}%")
    logger.info(f"[COMPARACAO] Resultados idênticos: {resultados['resultados_identicos']}/{resultados['amostras_testadas']}")
    
    if resultados["casos_divergentes"]:
        logger.warning(f"[COMPARACAO] {resultados['resultados_diferentes']} casos divergentes encontrados")
    
    return resultados

def verificar(
    db_path: str = DB_PATH,
    max_workers: int = MAX_WORKERS,
    batch_size: int = 500
) -> None:
    """
    Orquestra a verificacao de XMLs e atualizacao do banco.
    Etapas:
        - Busca registros noo baixados
        - Verifica existência dos XMLs no disco
        - Atualiza status xml_baixado = 1 no banco

    Args:
        db_path: Caminho do banco SQLite.
        max_workers: Numero de threads para paralelismo.
        batch_size: Tamanho do lote para logs e atualizacao.

    Raises:
        Exception: Se ocorrer erro inesperado durante o processo.
    """
    logger.info("[verificacao] Iniciando verificacao de XMLs no disco...")
    chaves_com_arquivo = verificar_arquivos_existentes(db_path=db_path, max_workers=max_workers, batch_size=batch_size)
    atualizar_status_no_banco(chaves_com_arquivo, db_path=db_path, batch_size=batch_size)
    logger.info("[verificacao] Processo de verificacao finalizado.")

# ------------------------------------------------------------------------------
# execucao direta (modo script)
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    # Configura logging para ver a execução
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('verificador_xmls.log')
        ]
    )
    
    # Ajusta o caminho do banco conforme o diretório de execução
    import os
    if os.path.basename(os.getcwd()) == 'src':
        # Se executando do diretório src, vai para o nível superior
        db_path = "../omie.db"
    else:
        # Se executando da raiz do projeto
        db_path = "omie.db"
    
    # Verifica se o banco existe
    if not os.path.exists(db_path):
        logger.error(f"[ERRO] Banco de dados não encontrado: {db_path}")
        logger.error(f"[ERRO] Diretório atual: {os.getcwd()}")
        logger.error("[ERRO] Execute o script da raiz do projeto ou verifique o caminho do banco")
        sys.exit(1)
    
    logger.info(f"[CONFIG] Usando banco de dados: {os.path.abspath(db_path)}")
    logger.info(f"[CONFIG] Usando versão {'OTIMIZADA' if USE_OPTIMIZED_VERSION else 'ORIGINAL'} das funções XML")
    
    # Pergunta se quer executar comparação primeiro
    import sys
    if "--comparar" in sys.argv or "--compare" in sys.argv:
        logger.info("[MODO] Executando comparação entre funções...")
        resultados_comparacao = comparar_funcoes_gerar_xml(db_path=db_path, num_amostras=200)
        
        print("\n" + "="*60)
        print("📊 RESULTADOS DA COMPARAÇÃO")
        print("="*60)
        print(f"🔢 Amostras testadas: {resultados_comparacao.get('amostras_testadas', 0)}")
        print(f" Tempo original: {resultados_comparacao.get('tempo_original', 0):.4f}s")
        print(f" Tempo otimizada: {resultados_comparacao.get('tempo_otimizada', 0):.4f}s")
        print(f"📈 Melhoria: {resultados_comparacao.get('melhoria_percentual', 0):+.1f}%")
        print(f"✅ Idênticos: {resultados_comparacao.get('resultados_identicos', 0)}/{resultados_comparacao.get('amostras_testadas', 0)}")
        print(f"⚠️  Diferentes: {resultados_comparacao.get('resultados_diferentes', 0)}")
        print(f"❌ Erros original: {resultados_comparacao.get('erros_original', 0)}")
        print(f"❌ Erros otimizada: {resultados_comparacao.get('erros_otimizada', 0)}")
        
        if resultados_comparacao.get('casos_divergentes'):
            print("\n Exemplos de casos divergentes:")
            for caso in resultados_comparacao['casos_divergentes']:
                print(f"   - {caso['chave']}: {caso['original']} != {caso['otimizada']}")
        
        print("="*60)
        
        # Pergunta se quer continuar com verificação normal
        try:
            continuar = input("\nDeseja continuar com a verificação normal? (s/N): ").lower()
            if continuar not in ['s', 'sim', 'y', 'yes']:
                logger.info("Encerrando após comparação.")
                sys.exit(0)
        except (KeyboardInterrupt, EOFError):
            logger.info("Encerrando.")
            sys.exit(0)
    
    # Permite configuracoo via env vars ou argumentos futuros
    verificar(db_path=db_path)
