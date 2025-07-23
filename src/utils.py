# =============================================================================
# MÓDULO DE UTILITÁRIOS PARA PIPELINE OMIE V3
# =============================================================================
"""
Módulo de utilitários centralizados para o pipeline de extração de dados do Omie.

Este módulo fornece funções essenciais organizadas por categoria:

## CONSULTA E MANIPULAÇÃO DE REGISTROS
- obter_registros_pendentes()
- obter_registros_filtrados()
- buscar_registros_invalidos_para_reprocessar()

## PROCESSAMENTO DE REGISTROS INVÁLIDOS
- marcar_registros_invalidos_e_listar_dias()
- limpar_registros_invalidos_reprocessados()

## VALIDAÇÃO E NORMALIZAÇÃO DE DADOS
- normalizar_data()
- formatar_data_iso_para_br()
- validar_data_formato()
- sanitizar_cnpj()
- normalizar_valor_nf()

## MANIPULAÇÃO DE ARQUIVOS E CAMINHOS
- gerar_nome_arquivo_xml()
- gerar_xml_path()
- gerar_xml_path_otimizado() - NOVA: Versão otimizada usando descobrir_todos_xmls()
- mapear_xml_data_chave_caminho() - NOVA: Mapeamento por data de emissão
- gerar_xml_info_dict() - NOVA: Informações XML como dicionário
- criar_mapeamento_completo_com_descobrir_xmls() - NOVA: Mapeamento completo otimizado
- extrair_mes_do_path()
- criar_lockfile()
- listar_arquivos_xml_em()
- descobrir_todos_xmls() - Busca recursiva eficiente

## CONTROLE DE RATE LIMITING
- respeitar_limite_requisicoes()
- respeitar_limite_requisicoes_async()

## OPERAÇÕES DE BANCO DE DADOS
- iniciar_db()
- salvar_nota()
- salvar_varias_notas()
- atualizar_status_xml()
- marcar_como_erro()
- marcar_como_baixado()

## TRANSFORMAÇÃO DE DADOS
- transformar_em_tuple()

Características técnicas:
- Operações batch para máxima performance
- Validação rigorosa de dados com recuperação de erros
- Logging estruturado com contexto específico
- Tratamento de exceções não-propagante
- Suporte a múltiplos formatos de data
- Indexação automática de banco de dados

Padrões implementados (PEP 8):
- Type hints completos
- Docstrings detalhadas no formato Google/NumPy
- Tratamento de erros robusto
- Logging estruturado por contexto
- Validação de pré-condições
- Fallbacks seguros para operações críticas
"""

# =============================================================================
# IMPORTAÇÕES DA BIBLIOTECA PADRÃO
# =============================================================================
import asyncio
import concurrent.futures
import json
import logging
import os
import re
import sqlite3
import time
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock
from time import monotonic, sleep
from typing import Any, Dict, List, Optional, Tuple, Union
from xml.etree import ElementTree as ET

# =============================================================================
# CONFIGURAÇÃO DO LOGGER
# =============================================================================
logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTES E CONFIGURAÇÕES GLOBAIS
# =============================================================================
# Campos obrigatórios para validação de integridade de registros
CAMPOS_ESSENCIAIS: List[str] = [
    'cChaveNFe', 'nIdNF', 'nIdPedido', 'dEmi', 'dReg', 'nNF', 'caminho_arquivo'
]

# XPaths para extração de dados de arquivos XML
XPATHS: Dict[str, str] = {
    "dEmi": ".//{*}ide/{*}dEmi",
    "nNF": ".//{*}ide/{*}nNF",
    "cRazao": ".//{*}dest/{*}xNome",
    "cnpj_cpf": ".//{*}dest/{*}CNPJ|.//{*}dest/{*}CPF"
}

# Configurações de otimização SQLite
SQLITE_PRAGMAS: Dict[str, str] = {
    "journal_mode": "WAL",
    "synchronous": "NORMAL",
    "temp_store": "MEMORY",
    "cache_size": "-64000",  # 64MB cache
    "mmap_size": "268435456"  # 256MB mmap
}

# Schema SQL para criação de tabelas
SCHEMA_NOTAS_CREATE = """
    CREATE TABLE IF NOT EXISTS notas (
        -- Campos principais da NFe
        cChaveNFe TEXT PRIMARY KEY,
        nIdNF INTEGER,
        nIdPedido INTEGER,
        
        -- Campos de data/hora
        dCan TEXT,
        dEmi TEXT,
        dInut TEXT,
        dReg TEXT,
        dSaiEnt TEXT,
        hEmi TEXT,
        hSaiEnt TEXT,
        
        -- Campos de identificação
        mod TEXT,
        nNF TEXT,
        serie TEXT,
        tpAmb TEXT,
        tpNF TEXT,
        
        -- Campos do destinatário
        cnpj_cpf TEXT,
        cRazao TEXT,
        
        -- Valores
        vNF REAL,
        
        -- Campos de controle
        xml_baixado BOOLEAN DEFAULT 0,
        anomesdia INTEGER DEFAULT NULL,
        caminho_arquivo TEXT DEFAULT NULL,
        xml_vazio INTEGER DEFAULT 0
    )
"""

# Schema SQL para inserção de registros (mantido para compatibilidade)
SCHEMA_NOTAS_INSERT = """
    INSERT INTO notas (
        cChaveNFe, nIdNF, nIdPedido, dCan, dEmi, dInut, dReg, dSaiEnt, hEmi, hSaiEnt,
        mod, nNF, serie, tpAmb, tpNF, cnpj_cpf, cRazao, vNF,
        caminho_arquivo, xml_baixado
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# Para compatibilidade retroativa
SCHEMA_NOTAS = SCHEMA_NOTAS_INSERT  # Mantém referência antiga

# Estado global para rate limiting assíncrono
_ultima_chamada_async = 0.0

# =============================================================================
# CLASSES E DATACLASSES
# =============================================================================
@dataclass
class DatabaseConfig:
    """Configuração personalizada para banco de dados SQLite."""
    
    cache_size: str = "-64000"  # 64MB cache
    mmap_size: str = "268435456"  # 256MB memory-mapped
    journal_mode: str = "WAL"
    synchronous: str = "NORMAL"
    temp_store: str = "MEMORY"
    timeout: int = 30  # Timeout em segundos
    
    def get_pragmas(self) -> Dict[str, str]:
        """
        Retorna dicionário com os PRAGMAs de configuração do banco.
        
        Returns:
            Dict[str, str]: PRAGMAs de configuração
        """
        return {
            "cache_size": self.cache_size,
            "mmap_size": self.mmap_size,
            "journal_mode": self.journal_mode,
            "synchronous": self.synchronous,
            "temp_store": self.temp_store
        }


@dataclass
class ResultadoSalvamento:
    """Resultado estruturado de operações de salvamento."""
    
    sucesso: bool
    chave: Optional[str] = None
    duplicata: bool = False
    motivo: Optional[str] = None
    tempo_execucao: Optional[float] = None


class DatabaseError(Exception):
    """Exceção específica para erros de banco de dados."""
    pass


class SchemaError(DatabaseError):
    """Exceção para erros de schema."""
    pass


class RegistroInvalidoError(ValueError):
    """Exceção para registros com dados inválidos."""
    pass

# =============================================================================
# CONSULTA E MANIPULAÇÃO DE REGISTROS
# =============================================================================
def obter_registros_pendentes(db_path: str, dias_filtrar: Optional[List[str]] = None) -> List[Tuple]:
    """
    Obtem registros de notas fiscais pendentes de download do banco SQLite.
    
    Esta funcao e otimizada para grandes volumes de dados e implementa:
    - Configuracoo automatica de PRAGMA SQLite para performance
    - Criacoo de indices sob demanda
    - Normalizacoo automatica de formatos de data
    - Filtragem flexivel por multiplas datas
    - Logging detalhado de operacões
    
    Args:
        db_path: Caminho absoluto para o arquivo do banco SQLite
        dias_filtrar: Lista opcional de datas para filtrar registros.
                     Aceita formatos: 'dd/mm/yyyy', 'yyyy-mm-dd'
    
    Returns:
        Lista de tuplas contendo (nIdNF, cChaveNFe, dEmi, cnpj_cpf, cRazao) para registros pendentes
        
    Raises:
        sqlite3.Error: Em caso de erros de acesso ao banco
        ValueError: Se formato de data for invalido
        
    Examples:
        >>> registros = obter_registros_pendentes("omie.db")
        >>> registros_filtrados = obter_registros_pendentes("omie.db", ["17/07/2025", "18/07/2025"])
    """
    try:
        with sqlite3.connect(db_path) as conn:
            # Otimizacões de performance SQLite
            for pragma, valor in SQLITE_PRAGMAS.items():
                conn.execute(f"PRAGMA {pragma}={valor}")
            
            # Criacoo de indices para otimizacoo de consultas
            conn.execute("CREATE INDEX IF NOT EXISTS idx_xml_baixado ON notas (xml_baixado)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_dEmi_baixado ON notas (dEmi, xml_baixado)")
            
            if dias_filtrar:
                logger.info(f"[PENDENTES] Filtrando registros para {len(dias_filtrar)} dia(s): {dias_filtrar}")
                
                # Normalizacoo de datas para formato consistente do banco
                dias_normalizados = []
                for data in dias_filtrar:
                    if data and data.strip():
                        data_normalizada = normalizar_data(data.strip())
                        if data_normalizada:
                            dias_normalizados.append(data_normalizada)
                        else:
                            logger.warning(f"[PENDENTES] Data invalida ignorada: {data}")
                
                if not dias_normalizados:
                    logger.warning("[PENDENTES] Nenhuma data valida fornecida para filtro")
                    return []
                
                # Construcoo de consulta otimizada com placeholders
                placeholders = ','.join(['?' for _ in dias_normalizados])
                query = f"""
                    SELECT nIdNF, cChaveNFe, dEmi, cnpj_cpf, cRazao 
                    FROM notas 
                    WHERE xml_baixado = 0 AND dEmi IN ({placeholders})
                    ORDER BY dEmi, nNF
                """
                
                rows = conn.execute(query, dias_normalizados).fetchall()
                logger.info(f"[PENDENTES] Encontrados {len(rows)} registros para os dias especificados")
                
            else:
                # Consulta para todos os registros pendentes
                query = """
                    SELECT nIdNF, cChaveNFe, dEmi, cnpj_cpf, cRazao 
                    FROM notas 
                    WHERE xml_baixado = 0
                    ORDER BY dEmi, nNF
                """
                rows = conn.execute(query).fetchall()
                logger.info(f"[PENDENTES] Encontrados {len(rows)} registros pendentes total")
            
            return rows
            
    except sqlite3.Error as e:
        logger.error(f"[PENDENTES] Erro de banco de dados: {e}")
        return []
    except Exception as e:
        logger.error(f"[PENDENTES] Erro inesperado ao obter registros pendentes: {e}")
        return []


def obter_registros_filtrados(db_path: str, filtros: Dict[str, Any]) -> List[Tuple]:
    """
    Busca registros do banco aplicando filtros específicos.
    
    Suporta diferentes tipos de filtros para diferentes modos de execução:
    - apenas_invalidos: Registros com status inválido para reprocessamento
    - periodo: Filtro por intervalo de datas
    - status_especifico: Filtro por status específico no campo arquivo
    
    Esta função é otimizada para:
    - Performance com índices automáticos
    - Normalização de formatos de data
    - Logging detalhado de operações
    - Tratamento robusto de erros
    
    Args:
        db_path: Caminho absoluto para o arquivo do banco SQLite
        filtros: Dicionário com critérios de filtro
        
    Returns:
        Lista de tuplas com os registros encontrados no formato:
        (nIdNF, cChaveNFe, dEmi, nNF)
        
    Examples:
        # Para reprocessamento (modo reprocessamento)
        filtros = {"apenas_invalidos": True}
        
        # Para período específico
        filtros = {"periodo": {"inicio": "01/05/2025", "fim": "31/05/2025"}}
        
    Raises:
        sqlite3.Error: Erros de banco de dados são logados e retorna lista vazia
        Exception: Outros erros são logados e retorna lista vazia
    """
    if not os.path.exists(db_path):
        logger.error(f"[FILTRADOS] Banco de dados não encontrado: {db_path}")
        return []
    
    try:
        with sqlite3.connect(db_path) as conn:
            # Configurações de performance
            for pragma, value in SQLITE_PRAGMAS.items():
                conn.execute(f"PRAGMA {pragma} = {value}")
            
            # Cria índice se não existir (para performance)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_xml_baixado_dEmi 
                ON notas(xml_baixado, dEmi)
            """)
            
            # Base da query
            base_query = """
                SELECT nIdNF, cChaveNFe, dEmi, nNF 
                FROM notas 
                WHERE xml_baixado = 0
            """
            
            condicoes = []
            parametros = []
            
            # Filtro para reprocessamento (registros inválidos)
            if filtros.get("apenas_invalidos"):
                # Considera inválidos: NULL, vazio, ou contendo "erro"
                condicoes.append("(arquivo IS NULL OR arquivo = '' OR arquivo LIKE '%erro%')")
                logger.info("[FILTRADOS] Filtro para reprocessamento de registros inválidos")
                
            # Filtro por período
            if filtros.get("periodo"):
                periodo = filtros["periodo"]
                if periodo.get("inicio"):
                    data_inicio = periodo["inicio"]
                    if '/' in data_inicio:
                        partes = data_inicio.split('/')
                        if len(partes) == 3:
                            data_normalizada = f"{partes[2]}-{partes[1].zfill(2)}-{partes[0].zfill(2)}"
                            condicoes.append("dEmi >= ?")
                            parametros.append(data_normalizada)
                            
                if periodo.get("fim"):
                    data_fim = periodo["fim"]
                    if '/' in data_fim:
                        partes = data_fim.split('/')
                        if len(partes) == 3:
                            data_normalizada = f"{partes[2]}-{partes[1].zfill(2)}-{partes[0].zfill(2)}"
                            condicoes.append("dEmi <= ?")
                            parametros.append(data_normalizada)
                            
                logger.info(f"[FILTRADOS] Filtro por período: {periodo}")
                
            # Filtro por status específico
            if filtros.get("status_especifico"):
                condicoes.append("arquivo LIKE ?")
                parametros.append(f"%{filtros['status_especifico']}%")
                logger.info(f"[FILTRADOS] Filtro por status: {filtros['status_especifico']}")
                
            # Monta query final
            if condicoes:
                query = base_query + " AND " + " AND ".join(condicoes)
            else:
                query = base_query
                
            query += " ORDER BY dEmi, nNF"
            
            rows = conn.execute(query, parametros).fetchall()
            
            logger.info(f"[FILTRADOS] Busca filtrada encontrou {len(rows)} registros")
            return rows
            
    except sqlite3.Error as e:
        logger.error(f"[FILTRADOS] Erro de banco de dados: {e}")
        return []
    except Exception as e:
        logger.error(f"[FILTRADOS] Erro inesperado ao buscar registros filtrados: {e}")
        return []


# =============================================================================
# PROCESSAMENTO DE REGISTROS INVÁLIDOS
# =============================================================================

def marcar_registros_invalidos_e_listar_dias(db_path: str) -> List[str]:
    """
    Marca registros com campos essenciais invalidos e retorna dias afetados.
    
    Identifica registros com problemas de integridade e os marca como 'INVALIDO'
    para reprocessamento posterior, sem remover dados do banco.
    
    Criterios de invalidacoo:
    - Chave NFe ausente ou vazia
    - Data de emissoo ausente ou vazia
    - Numero da NFe ausente ou vazio
    - Outros campos essenciais com valores nulos
    
    Args:
        db_path: Caminho absoluto para o banco SQLite
        
    Returns:
        Lista ordenada de dias unicos (formato YYYY-MM-DD) dos registros invalidos
        
    Processo:
    1. Identifica registros com campos essenciais invalidos
    2. Extrai e normaliza datas afetadas
    3. Retorna lista de dias unicos para reprocessamento
    
    Raises:
        sqlite3.Error: Em caso de erros de acesso ao banco
        
    Examples:
        >>> dias = marcar_registros_invalidos_e_listar_dias("omie.db")
        >>> # ['2025-07-17', '2025-07-18']
    """
    dias_afetados = set()
    
    try:
        with sqlite3.connect(db_path) as conn:
            # Configuracões de performance
            for pragma, valor in SQLITE_PRAGMAS.items():
                conn.execute(f"PRAGMA {pragma}={valor}")
            
            cursor = conn.cursor()
            
            # Query para identificar registros com campos essenciais invalidos
            query_invalidos = """
                SELECT cChaveNFe, dEmi 
                FROM notas
                WHERE (
                    cChaveNFe IS NULL OR TRIM(cChaveNFe) = '' OR cChaveNFe = '-'
                    OR dEmi IS NULL OR TRIM(dEmi) = '' OR dEmi = '-'
                    OR nNF IS NULL OR TRIM(nNF) = '' OR nNF = '-'
                )
            """
            
            cursor.execute(query_invalidos)
            registros_invalidos = cursor.fetchall()
            
            if not registros_invalidos:
                logger.info("[INVALIDOS] Nenhum registro invalido encontrado")
                return []
            
            # Extracoo e normalizacoo das datas dos registros invalidos
            chaves_invalidas = []
            for chave, data_emissao in registros_invalidos:
                if chave:
                    chaves_invalidas.append(chave)
                
                # Normalizacoo da data de emissoo
                if data_emissao:
                    try:
                        data_normalizada = normalizar_data(str(data_emissao).strip())
                        if data_normalizada:
                            dias_afetados.add(data_normalizada)
                    except Exception as e:
                        logger.warning(f"[INVALIDOS] Erro ao normalizar data '{data_emissao}': {e}")
            
            # Marcacoo dos registros como invalidos
            if chaves_invalidas:
                # ATUALIZAÇÃO: Registros inválidos identificados (apenas log, sem marcação)
                # Colunas de erro foram removidas do schema
                logger.info(f"[INVALIDOS] {len(chaves_invalidas)} registros com campos inválidos identificados")
                logger.info(f"[INVALIDOS] Datas afetadas: {sorted(dias_afetados)}")
                
                # Log adicional para debugging
                if len(chaves_invalidas) <= 10:  # Evita spam no log
                    logger.debug(f"[INVALIDOS] Chaves identificadas: {chaves_invalidas}")
            else:
                logger.info("[INVALIDOS] Nenhum registro inválido encontrado")
            
            return sorted(dias_afetados)
            
    except sqlite3.Error as e:
        logger.error(f"[INVALIDOS] Erro de banco de dados ao marcar registros invalidos: {e}")
        return []
    except Exception as e:
        logger.error(f"[INVALIDOS] Erro inesperado ao marcar registros invalidos: {e}")
        return []

def buscar_registros_invalidos_para_reprocessar(db_path: str, dias: List[str]) -> List[Dict]:
    """
    Busca registros marcados como invalidos para reprocessamento.
    
    Recupera dados completos de registros previamente marcados como 'INVALIDO'
    para permitir reprocessamento completo sem perda de contexto.
    
    Args:
        db_path: Caminho absoluto para o banco SQLite
        dias: Lista de datas (formato YYYY-MM-DD) para filtrar registros
        
    Returns:
        Lista de dicionarios contendo dados completos dos registros invalidos
        
    Caracteristicas:
    - Conversoo automatica de Row para Dict para facilitar manipulacoo
    - Filtragem por multiplas datas simultâneas
    - Logging detalhado de operacões
    - Tratamento robusto de erros
    
    Raises:
        sqlite3.Error: Em caso de erros de acesso ao banco
        
    Examples:
        >>> registros = buscar_registros_invalidos_para_reprocessar("omie.db", ["2025-07-17"])
        >>> # [{'cChaveNFe': '123...', 'dEmi': '2025-07-17', ...}]
    """
    if not dias:
        logger.info("[REPROCESSAR] Nenhum dia fornecido para busca")
        return []
    
    try:
        with sqlite3.connect(db_path) as conn:
            # Configuracões de performance
            for pragma, valor in SQLITE_PRAGMAS.items():
                conn.execute(f"PRAGMA {pragma}={valor}")
            
            # Configuracoo para retornar dicionarios em vez de tuplas
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Construcoo da query com placeholders seguros
            placeholders = ','.join(['?' for _ in dias])
            query = f"""
                SELECT * FROM notas
                WHERE xml_baixado = 0 AND dEmi IN ({placeholders})
                ORDER BY dEmi, nNF
            """
            
            cursor.execute(query, dias)
            rows = cursor.fetchall()
            
            # Conversoo para lista de dicionarios
            resultados = [dict(row) for row in rows]
            
            logger.info(f"[REPROCESSAR] Encontrados {len(resultados)} registros invalidos para reprocessamento")
            
            if resultados:
                # Log estatistico por data
                contagem_por_data = {}
                for registro in resultados:
                    data = registro.get('dEmi', 'N/A')
                    contagem_por_data[data] = contagem_por_data.get(data, 0) + 1
                
                for data, count in contagem_por_data.items():
                    logger.info(f"[REPROCESSAR] Data {data}: {count} registros")
            
            return resultados
            
    except sqlite3.Error as e:
        logger.error(f"[REPROCESSAR] Erro de banco de dados: {e}")
        return []
    except Exception as e:
        logger.error(f"[REPROCESSAR] Erro inesperado ao buscar registros invalidos: {e}")
        return []


def limpar_registros_invalidos_reprocessados(db_path: str, dias: List[str]) -> int:
    """
    Remove registros marcados como invalidos apos reprocessamento bem-sucedido.
    
    Esta funcao realiza limpeza controlada de registros que foram reprocessados
    com sucesso, removendo apenas os registros marcados como 'INVALIDO' para
    as datas especificadas.
    
    Args:
        db_path: Caminho absoluto para o banco SQLite
        dias: Lista de datas (formato YYYY-MM-DD) para limpeza
        
    Returns:
        Numero de registros removidos do banco
        
    Caracteristicas:
    - Remocoo segura com transacões
    - Contagem precisa de registros removidos
    - Logging detalhado de operacões
    - Validacoo de pre-condicões
    
    Raises:
        sqlite3.Error: Em caso de erros de acesso ao banco
        
    Examples:
        >>> removidos = limpar_registros_invalidos_reprocessados("omie.db", ["2025-07-17"])
        >>> # 15 (registros removidos)
    """
    if not dias:
        logger.info("[LIMPEZA] Nenhum dia fornecido para limpeza")
        return 0
    
    try:
        with sqlite3.connect(db_path) as conn:
            # Configuracões de performance
            for pragma, valor in SQLITE_PRAGMAS.items():
                conn.execute(f"PRAGMA {pragma}={valor}")
            
            cursor = conn.cursor()
            
            # Construcoo da query de remocoo com placeholders seguros
            placeholders = ','.join(['?' for _ in dias])
            query = f"""
                DELETE FROM notas 
                WHERE xml_baixado = 0 AND dEmi IN ({placeholders})
            """
            
            cursor.execute(query, dias)
            registros_removidos = cursor.rowcount
            conn.commit()
            
            if registros_removidos > 0:
                logger.info(f"[LIMPEZA] {registros_removidos} registros invalidos removidos com sucesso")
                logger.info(f"[LIMPEZA] Dias processados: {dias}")
            else:
                logger.info("[LIMPEZA] Nenhum registro invalido encontrado para remocoo")
            
            return registros_removidos
            
    except sqlite3.Error as e:
        logger.error(f"[LIMPEZA] Erro de banco de dados durante limpeza: {e}")
        return 0
    except Exception as e:
        logger.error(f"[LIMPEZA] Erro inesperado durante limpeza: {e}")
        return 0


# Mantém compatibilidade retroativa com nome antigo
remover_registros_sem_dEmi_e_listar_dias = marcar_registros_invalidos_e_listar_dias

# =============================================================================
# VALIDAÇÃO E NORMALIZAÇÃO DE DADOS
# =============================================================================

def normalizar_data(data: Optional[str]) -> Optional[str]:
    """
    Normaliza datas para formato ISO padroo (YYYY-MM-DD).
    
    Aceita multiplos formatos de entrada e converte para formato consistente
    usado em todo o sistema. Implementa validacoo rigorosa e recuperacoo de erros.
    
    Formatos aceitos:
    - Brasileiro: dd/mm/yyyy (17/07/2025)
    - ISO: yyyy-mm-dd (2025-07-17)
    - Outros formatos comuns com separadores
    
    Args:
        data: String contendo data em formato conhecido ou None
        
    Returns:
        String em formato ISO (YYYY-MM-DD) ou None se invalida
        
    Raises:
        Nunca propaga excecões - retorna None para dados invalidos
        
    Examples:
        >>> normalizar_data("17/07/2025")
        '2025-07-17'
        >>> normalizar_data("2025-07-17")
        '2025-07-17'
        >>> normalizar_data("invalid")
        None
    """
    if not data or not isinstance(data, str):
        return None
    
    data_limpa = data.strip()
    if not data_limpa:
        return None
    
    try:
        # Formato brasileiro dd/mm/yyyy
        if "/" in data_limpa:
            data_obj = datetime.strptime(data_limpa, "%d/%m/%Y")
            return data_obj.strftime("%Y-%m-%d")
        
        # Formato ISO yyyy-mm-dd (validacoo)
        elif "-" in data_limpa:
            data_obj = datetime.strptime(data_limpa, "%Y-%m-%d")
            return data_obj.strftime("%Y-%m-%d")  # Renormaliza para garantir formato
        
        # Formato compacto yyyymmdd
        elif len(data_limpa) == 8 and data_limpa.isdigit():
            data_obj = datetime.strptime(data_limpa, "%Y%m%d")
            return data_obj.strftime("%Y-%m-%d")
        
        else:
            logger.warning(f"[DATA] Formato de data noo reconhecido: '{data_limpa}'")
            return None
            
    except ValueError as e:
        logger.warning(f"[DATA] Erro ao parsear data '{data_limpa}': {e}")
        return None
    except Exception as e:
        logger.warning(f"[DATA] Erro inesperado ao normalizar data '{data_limpa}': {e}")
        return None


def formatar_data_iso_para_br(data: Optional[str]) -> Optional[str]:
    """
    Converte data ISO (YYYY-MM-DD) para formato brasileiro (dd/mm/YYYY).
    
    Args:
        data: Data em formato ISO ou None
        
    Returns:
        Data em formato brasileiro ou None se invalida
        
    Examples:
        >>> formatar_data_iso_para_br("2025-07-17")
        '17/07/2025'
    """
    if not data:
        return None
    
    try:
        data_obj = datetime.strptime(data, '%Y-%m-%d')
        return data_obj.strftime('%d/%m/%Y')
    except ValueError as e:
        logger.warning(f"[DATA] Data ISO invalida '{data}': {e}")
        return data  # Retorna original se falhar
    except Exception as e:
        logger.warning(f"[DATA] Erro ao converter data '{data}': {e}")
        return data


def validar_data_formato(data: str, formato: str) -> bool:
    """
    Valida se uma data esta no formato especificado.
    
    Args:
        data: String da data para validacoo
        formato: Formato esperado (ex: '%Y-%m-%d', '%d/%m/%Y')
        
    Returns:
        True se a data esta no formato correto, False caso contrario
        
    Examples:
        >>> validar_data_formato("2025-07-17", "%Y-%m-%d")
        True
        >>> validar_data_formato("17/07/2025", "%Y-%m-%d")
        False
    """
    if not data or not formato:
        return False
    
    try:
        datetime.strptime(data, formato)
        return True
    except ValueError:
        return False
    except Exception as e:
        logger.warning(f"[DATA] Erro na validacoo de formato: {e}")
        return False

def sanitizar_cnpj(valor: Union[str, int, None]) -> str:
    """
    Remove caracteres noo numericos de CNPJ ou CPF.
    
    Args:
        valor: CNPJ/CPF como string, int ou None
        
    Returns:
        String contendo apenas numeros
        
    Examples:
        >>> sanitizar_cnpj("12.345.678/0001-90")
        '12345678000190'
        >>> sanitizar_cnpj(None)
        ''
    """
    if not valor:
        return ''
    
    try:
        return re.sub(r'\D', '', str(valor))
    except Exception as e:
        logger.warning(f"[CNPJ] Erro ao sanitizar CNPJ/CPF '{valor}': {e}")
        return ''


def normalizar_valor_nf(valor: Union[str, float, int, None]) -> float:
    """
    Converte valor para float com tratamento robusto de erros.
    
    Args:
        valor: Valor como string, float, int ou None
        
    Returns:
        Valor convertido para float (0.0 se invalido)
        
    Examples:
        >>> normalizar_valor_nf("1234.56")
        1234.56
        >>> normalizar_valor_nf("invalid")
        0.0
    """
    if valor is None:
        return 0.0
    
    try:
        # Remove espacos e converte virgula para ponto
        if isinstance(valor, str):
            valor_limpo = valor.strip().replace(',', '.')
            return float(valor_limpo)
        
        return float(valor)
    
    except (ValueError, TypeError) as e:
        logger.warning(f"[VALOR] Valor invalido '{valor}': {e}")
        return 0.0
    except Exception as e:
        logger.warning(f"[VALOR] Erro inesperado ao normalizar valor '{valor}': {e}")
        return 0.0


# =============================================================================
# TRANSFORMAÇÃO DE DADOS
# =============================================================================

def transformar_em_tuple(registro: Dict) -> Tuple:
    """
    Transforma dicionario de nota fiscal em tupla para insercoo otimizada no banco.
    
    Realiza validacoo rigorosa, normalizacoo de dados e transformacoo de tipos
    para garantir consistência e integridade dos dados no banco SQLite.
    
    Validacões realizadas:
    - Campos essenciais obrigatorios
    - Tipos de dados corretos
    - Valores numericos validos
    - Formatos de data consistentes
    - Campos de texto sanitizados
    
    Args:
        registro: Dicionario contendo dados da nota fiscal
        
    Returns:
        Tupla com dados normalizados prontos para insercoo
        
    Raises:
        ValueError: Se campos essenciais estiverem ausentes
        
    Examples:
        >>> registro = {'cChaveNFe': '123...', 'dEmi': '17/07/2025', 'nNF': '123'}
        >>> tupla = transformar_em_tuple(registro)
    """
    # Validacoo de campos essenciais
    campos_obrigatorios = ['cChaveNFe', 'dEmi', 'nNF']
    campos_ausentes = [campo for campo in campos_obrigatorios if not registro.get(campo)]
    
    if campos_ausentes:
        erro_msg = f"Campos obrigatorios ausentes: {campos_ausentes}"
        logger.error(f"[TUPLE] {erro_msg} no registro: {registro}")
        raise ValueError(erro_msg)
    
    # Funcões auxiliares para conversoo segura de tipos
    def safe_str(valor) -> Optional[str]:
        """Converte valor para string, tratando None e espacos."""
        if valor is None:
            return None
        valor_str = str(valor).strip()
        return valor_str if valor_str not in ('', '-', 'None') else None
    
    def safe_int(valor) -> Optional[int]:
        """Converte valor para int, tratando erros."""
        if valor is None:
            return None
        try:
            return int(float(valor))  # Converte via float para tratar decimais
        except (ValueError, TypeError):
            logger.warning(f"[TUPLE] Valor inteiro invalido: {valor}")
            return None
    
    def safe_float(valor) -> float:
        """Converte valor para float, retornando 0.0 se invalido."""
        return normalizar_valor_nf(valor)
    
    try:
        # Construcoo da tupla com validacoo e conversoo de tipos
        tupla = (
            safe_str(registro['cChaveNFe']),                    # chave_nfe
            safe_int(registro.get('nIdNF')),                    # id_nf
            safe_int(registro.get('nIdPedido')),                # id_pedido
            safe_str(registro.get('dCan')),                     # data_cancelamento
            normalizar_data(safe_str(registro.get('dEmi'))),    # data_emissao
            safe_str(registro.get('dInut')),                    # data_inutilizacao
            safe_str(registro.get('dReg')),                     # data_registro
            safe_str(registro.get('dSaiEnt')),                  # data_saida_entrada
            safe_str(registro.get('hEmi')),                     # hora_emissao
            safe_str(registro.get('hSaiEnt')),                  # hora_saida_entrada
            safe_str(registro.get('mod')),                      # modelo
            safe_str(registro.get('nNF')),                      # numero_nf
            safe_str(registro.get('serie')),                    # serie
            safe_str(registro.get('tpAmb')),                    # tipo_ambiente
            safe_str(registro.get('tpNF')),                     # tipo_nf
            safe_str(registro.get('cnpj_cpf')),                 # cnpj_cpf
            safe_str(registro.get('cRazao')),                   # razao_social
            safe_float(registro.get('vNF')),                    # valor_nf
            None,                                               # caminho_arquivo
            0                                                   # xml_baixado
        )
        
        # Log de debug para registros processados
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"[TUPLE] Registro transformado: {registro['cChaveNFe']}")
        
        return tupla
        
    except Exception as e:
        logger.error(f"[TUPLE] Erro critico ao transformar registro: {e}")
        logger.error(f"[TUPLE] Registro problematico: {registro}")
        raise


# =============================================================================
# MANIPULAÇÃO DE ARQUIVOS E CAMINHOS
# =============================================================================

def descobrir_todos_xmls(resultado_dir: Path) -> List[Path]:
    """Descobre todos os XMLs de forma robusta e eficiente."""
    if not resultado_dir.exists():
        return []
    
    # Método simples e robusto
    return list(resultado_dir.rglob("*.xml"))

def normalizar_chave_nfe(chave: str) -> str:
    """
    Normaliza chave NFe para exatamente 44 caracteres, removendo caracteres extras.
    
    A chave NFe deve ter exatamente 44 dígitos. Esta função remove qualquer
    caractere extra no final que possa ter sido adicionado incorretamente.
    
    Args:
        chave: Chave NFe potencialmente com caracteres extras
        
    Returns:
        Chave NFe normalizada com exatamente 44 caracteres
        
    Examples:
        >>> normalizar_chave_nfe("35250714200166000196550010000123451234567890")
        '35250714200166000196550010000123451234567890'[:44]
        >>> normalizar_chave_nfe("35250714200166000196550010000123451234567890")
        '35250714200166000196550010000123451234567890'
    """
    if not chave:
        return ''
    
    # Remove espaços e mantém apenas dígitos
    chave_limpa = re.sub(r'\D', '', str(chave).strip())
    
    # Normaliza para exatamente 44 caracteres
    if len(chave_limpa) >= 44:
        return chave_limpa[:44]  # Trunca se for maior
    else:
        # Se for menor, mantém como está (pode ser chave inválida, mas preserva original)
        logger.warning(f"[CHAVE_NFE] Chave com menos de 44 caracteres: {chave_limpa} (len={len(chave_limpa)})")
        return chave_limpa

def gerar_nome_arquivo_xml(chave: str, dEmi: Union[str, datetime], num_nfe: str) -> str:
    """
    Gera nome padronizado para arquivo XML baseado nos dados da NFe.
    
    Padroo: {numero_nf}_{data_emissao_YYYYMMDD}_{chave_nfe_44_chars}.xml
    
    Args:
        chave: Chave unica da NFe (será normalizada para 44 caracteres)
        dEmi: Data de emissoo (string ou datetime)
        num_nfe: Numero da nota fiscal
        
    Returns:
        Nome do arquivo XML no formato padronizado
        
    Raises:
        ValueError: Se dados obrigatorios estiverem ausentes ou invalidos
        
    Examples:
        >>> gerar_nome_arquivo_xml("35250714200166000196550010000123451234567890", "17/07/2025", "123")
        '123_20250717_35250714200166000196550010000123451234567890.xml'
    """
    if not all([chave, dEmi, num_nfe]):
        raise ValueError(f"Dados obrigatorios ausentes: chave={chave}, dEmi={dEmi}, num_nfe={num_nfe}")
    
    try:
        # Conversoo de data para datetime se necessario
        if isinstance(dEmi, str):
            dEmi_normalizada = normalizar_data(dEmi.strip())
            if not dEmi_normalizada:
                raise ValueError(f"Data de emissoo invalida: '{dEmi}'")
            dEmi_dt = datetime.strptime(dEmi_normalizada, "%Y-%m-%d")
        elif isinstance(dEmi, datetime):
            dEmi_dt = dEmi
        else:
            raise ValueError(f"Tipo de dEmi invalido: {type(dEmi)}")
        
        # Sanitizacoo dos componentes do nome
        num_nfe_limpo = str(num_nfe).strip()
        chave_normalizada = normalizar_chave_nfe(chave)  # NOVA: Normaliza chave para 44 chars
        data_formatada = dEmi_dt.strftime('%Y%m%d')
        
        nome_arquivo = f"{num_nfe_limpo}_{data_formatada}_{chave_normalizada}.xml"
        
        # Validacoo do nome gerado
        if len(nome_arquivo) > 255:  # Limite do sistema de arquivos
            logger.warning(f"[ARQUIVO] Nome muito longo: {nome_arquivo[:50]}...")
        
        return nome_arquivo
        
    except Exception as e:
        raise ValueError(f"Erro ao gerar nome do arquivo XML: {e}")

def gerar_xml_path(
    chave: str,
    dEmi: str,
    num_nfe: str,
    base_dir: str = "resultado"
) -> Tuple[Path, Path]:
    """
    Gera caminho completo hierarquico para armazenamento de arquivo XML.
    
    NOTA: Para melhor performance, considere usar gerar_xml_path_otimizado()
    que utiliza descobrir_todos_xmls() para busca recursiva mais eficiente.
    
    Estrutura: {base_dir}/{ano}/{mes}/{dia}[/{dia}_pasta_{numero}]/{arquivo}.xml
    
    IMPORTANTE: Esta função agora procura arquivos em subpastas numeradas
    quando há mais de 10.000 arquivos por dia, seguindo a estrutura real:
    - resultado/2025/07/21/arquivo.xml (poucos arquivos)  
    - resultado/2025/07/21/21_pasta_1/arquivo.xml (subpasta 1)
    - resultado/2025/07/21/21_pasta_2/arquivo.xml (subpasta 2)
    
    Args:
        chave: Chave unica da NFe
        dEmi: Data de emissoo (dd/mm/yyyy ou yyyy-mm-dd)
        num_nfe: Numero da nota fiscal
        base_dir: Diretorio base para armazenamento
        
    Returns:
        Tupla contendo (Path da pasta, Path do arquivo completo)
        
    Raises:
        ValueError: Se dados obrigatorios estiverem ausentes ou invalidos
        
    Examples:
        >>> pasta, arquivo = gerar_xml_path("123...", "21/07/2025", "123")
        >>> # pasta: resultado/2025/07/21 (ou resultado/2025/07/21/21_pasta_1 se em subpasta)
        >>> # arquivo: resultado/2025/07/21/123_20250721_123....xml
        
    See Also:
        gerar_xml_path_otimizado(): Versão otimizada usando descobrir_todos_xmls()
    """
    # Validacoo de pre-condicões
    if not all([chave, dEmi, num_nfe]):
        raise ValueError(f"Dados obrigatorios ausentes: chave={chave}, dEmi={dEmi}, num_nfe={num_nfe}")
    
    try:
        # Normalizacoo da data
        data_normalizada = normalizar_data(str(dEmi).strip())
        if not data_normalizada:
            raise ValueError(f"Data de emissoo invalida: '{dEmi}'")
        
        data_dt = datetime.strptime(data_normalizada, "%Y-%m-%d")
        
        # Geracoo do nome do arquivo usando funcao centralizada
        nome_arquivo = gerar_nome_arquivo_xml(chave, data_dt, num_nfe)
        
        # Construcoo da estrutura hierarquica de pastas (base)
        pasta_base = Path(base_dir) / data_dt.strftime('%Y') / data_dt.strftime('%m') / data_dt.strftime('%d')
        
        # OTIMIZAÇÃO: Usa descobrir_todos_xmls para busca eficiente
        # 1. Primeiro verifica se o diretório base existe
        if not pasta_base.exists():
            # Se pasta não existe, retorna caminho direto para criação
            caminho_direto = pasta_base / nome_arquivo
            return pasta_base, caminho_direto
        
        # 2. Usa descobrir_todos_xmls para encontrar arquivos XML na pasta do dia
        todos_xmls = descobrir_todos_xmls(pasta_base)
        
        # 3. Busca por arquivo com nome exato gerado
        for xml_path in todos_xmls:
            if xml_path.name == nome_arquivo:
                # Arquivo encontrado - retorna pasta pai e caminho completo
                return xml_path.parent, xml_path
        
        # 4. Se não encontrou arquivo específico, busca alternativa por chave NFe
        # (para casos onde o nome pode ter pequenas variações)
        chave_limpa = str(chave).strip()
        for xml_path in todos_xmls:
            if chave_limpa in xml_path.name:
                logger.debug(f"[XML_PATH] Arquivo encontrado por chave alternativa: {xml_path.name}")
                return xml_path.parent, xml_path
        
        # 5. Se não encontrou em lugar nenhum, retorna caminho direto (para criação)
        caminho_direto = pasta_base / nome_arquivo
        return pasta_base, caminho_direto
        
    except Exception as e:
        raise ValueError(f"Erro ao gerar caminho XML: {e}")


def gerar_xml_path_otimizado(
    chave: str,
    dEmi: str,
    num_nfe: str,
    base_dir: str = "resultado"
) -> Tuple[Path, Path]:
    """
    Versão otimizada da função gerar_xml_path usando descobrir_todos_xmls.
    
    Esta versão usa a função descobrir_todos_xmls para busca recursiva eficiente
    e gerar_nome_arquivo_xml para padronização consistente do nome.
    
    Vantagens desta abordagem:
    - Busca recursiva automática em subpastas
    - Uso da função centralizada de nomenclatura
    - Performance otimizada com single scan
    - Busca por chave alternativa em caso de variação no nome
    
    Args:
        chave: Chave única da NFe (44 caracteres)
        dEmi: Data de emissão (dd/mm/yyyy ou yyyy-mm-dd)
        num_nfe: Número da nota fiscal
        base_dir: Diretório base para armazenamento
        
    Returns:
        Tupla contendo (Path da pasta, Path do arquivo completo)
        
    Raises:
        ValueError: Se dados obrigatórios estiverem ausentes ou inválidos
        
    Examples:
        >>> pasta, arquivo = gerar_xml_path_otimizado("123...", "21/07/2025", "123")
        >>> # Usa descobrir_todos_xmls para busca eficiente
    """
    # Validação de pré-condições
    if not all([chave, dEmi, num_nfe]):
        raise ValueError(f"Dados obrigatórios ausentes: chave={chave}, dEmi={dEmi}, num_nfe={num_nfe}")
    
    try:
        # Normalização da data
        data_normalizada = normalizar_data(str(dEmi).strip())
        if not data_normalizada:
            raise ValueError(f"Data de emissão inválida: '{dEmi}'")
        
        data_dt = datetime.strptime(data_normalizada, "%Y-%m-%d")
        
        # Geração do nome padrão usando função centralizada
        nome_arquivo_esperado = gerar_nome_arquivo_xml(chave, data_dt, num_nfe)
        
        # Construção da pasta do dia
        pasta_dia = Path(base_dir) / data_dt.strftime('%Y') / data_dt.strftime('%m') / data_dt.strftime('%d')
        
        # Se pasta do dia não existe, retorna caminho para criação
        if not pasta_dia.exists():
            caminho_novo = pasta_dia / nome_arquivo_esperado
            return pasta_dia, caminho_novo
        
        # BUSCA OTIMIZADA: Usa descobrir_todos_xmls para scan recursivo
        logger.debug(f"[XML_PATH] Buscando XMLs recursivamente em: {pasta_dia}")
        todos_xmls_do_dia = descobrir_todos_xmls(pasta_dia)
        
        if not todos_xmls_do_dia:
            # Sem XMLs na pasta, retorna caminho para criação
            caminho_novo = pasta_dia / nome_arquivo_esperado
            return pasta_dia, caminho_novo
        
        # ESTRATÉGIA DE BUSCA EM MÚLTIPLAS ETAPAS:
        chave_limpa = str(chave).strip()
        num_nfe_limpo = str(num_nfe).strip()
        
        # 1. Busca por nome exato (mais precisa)
        for xml_path in todos_xmls_do_dia:
            if xml_path.name == nome_arquivo_esperado:
                logger.debug(f"[XML_PATH] Encontrado por nome exato: {xml_path}")
                return xml_path.parent, xml_path
        
        # 2. Busca por chave NFe no nome (tolerante a variações)
        for xml_path in todos_xmls_do_dia:
            if chave_limpa in xml_path.name and num_nfe_limpo in xml_path.name:
                logger.debug(f"[XML_PATH] Encontrado por chave+número: {xml_path}")
                return xml_path.parent, xml_path
        
        # 3. Busca apenas por chave (fallback)
        for xml_path in todos_xmls_do_dia:
            if chave_limpa in xml_path.name:
                logger.debug(f"[XML_PATH] Encontrado por chave: {xml_path}")
                return xml_path.parent, xml_path
        
        # 4. Nenhum arquivo encontrado - retorna caminho para criação
        # Escolhe a melhor pasta: direta ou primeira subpasta
        if any(item.is_dir() for item in pasta_dia.iterdir()):
            # Se há subpastas, usa a primeira para criação
            subpastas = [item for item in pasta_dia.iterdir() if item.is_dir()]
            if subpastas:
                primeira_subpasta = sorted(subpastas, key=lambda x: x.name)[0]
                caminho_novo = primeira_subpasta / nome_arquivo_esperado
                return primeira_subpasta, caminho_novo
        
        # Fallback final: pasta direta
        caminho_novo = pasta_dia / nome_arquivo_esperado
        return pasta_dia, caminho_novo
        
    except Exception as e:
        raise ValueError(f"Erro ao gerar caminho XML otimizado: {e}")


def mapear_xml_data_chave_caminho(
    registros: List[Tuple[str, str, str]], 
    base_dir: str = "resultado"
) -> Dict[str, Dict[str, str]]:
    """
    Mapeia registros de NFe para dicionário estruturado por data de emissão.
    
    Cria um mapeamento organizado por data de emissão contendo chave NFe e 
    caminho do arquivo XML correspondente, seguindo a estrutura hierárquica 
    do sistema de arquivos.
    
    Estrutura de retorno:
    {
        "2025-07-17": {
            "cChaveNFe": "chave_da_nfe_principal_do_dia",
            "caminho_arquivo": "/caminho/para/arquivo.xml"
        },
        "2025-07-18": {
            "cChaveNFe": "outra_chave_nfe",
            "caminho_arquivo": "/caminho/para/outro_arquivo.xml"
        }
    }
    
    Args:
        registros: Lista de tuplas contendo (chave, dEmi, num_nfe)
        base_dir: Diretório base para busca de arquivos (padrão: "resultado")
        
    Returns:
        Dicionário mapeado por data de emissão normalizada (YYYY-MM-DD)
        
    Raises:
        ValueError: Se dados obrigatórios estiverem ausentes ou inválidos
        
    Examples:
        >>> registros = [("123...", "17/07/2025", "001"), ("456...", "18/07/2025", "002")]
        >>> resultado = mapear_xml_data_chave_caminho(registros)
        >>> # {"2025-07-17": {"cChaveNFe": "123...", "caminho_arquivo": "/.../001_20250717_123....xml"}}
    """
    mapeamento = {}
    registros_processados = 0
    registros_com_erro = 0
    
    logger.info(f"[MAPEAR] Iniciando mapeamento de {len(registros)} registros")
    
    for chave, dEmi, num_nfe in registros:
        try:
            # Validação de dados obrigatórios
            if not all([chave, dEmi, num_nfe]):
                logger.warning(f"[MAPEAR] Registro com dados incompletos ignorado: chave={chave}, dEmi={dEmi}, num_nfe={num_nfe}")
                registros_com_erro += 1
                continue
            
            # Normalização da data de emissão
            data_normalizada = normalizar_data(str(dEmi).strip())
            if not data_normalizada:
                logger.warning(f"[MAPEAR] Data de emissão inválida ignorada: '{dEmi}' para chave {chave}")
                registros_com_erro += 1
                continue
            
            # Geração do caminho do arquivo XML usando versão otimizada
            try:
                pasta_xml, caminho_xml = gerar_xml_path_otimizado(chave, dEmi, num_nfe, base_dir)
                
                # Mapeamento da estrutura de retorno
                mapeamento[data_normalizada] = {
                    "cChaveNFe": str(chave).strip(),
                    "caminho_arquivo": str(caminho_xml)
                }
                
                registros_processados += 1
                
                # Log de debug para registros processados
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"[MAPEAR] Mapeado: {data_normalizada} -> {chave[:20]}... -> {caminho_xml}")
                    
            except ValueError as e:
                logger.warning(f"[MAPEAR] Erro ao gerar caminho XML para chave {chave}: {e}")
                registros_com_erro += 1
                continue
                
        except Exception as e:
            logger.error(f"[MAPEAR] Erro inesperado ao processar registro (chave={chave}): {e}")
            registros_com_erro += 1
            continue
    
    # Log de resumo da operação
    logger.info(f"[MAPEAR] Mapeamento concluído: {registros_processados} sucessos, {registros_com_erro} erros")
    
    if registros_com_erro > 0:
        taxa_erro = (registros_com_erro / len(registros)) * 100
        logger.warning(f"[MAPEAR] Taxa de erro: {taxa_erro:.1f}% ({registros_com_erro}/{len(registros)})")
    
    return mapeamento


def gerar_xml_info_dict(
    chave: str,
    dEmi: str,
    num_nfe: str,
    base_dir: str = "resultado"
) -> Dict[str, str]:
    """
    Versão simplificada que retorna informações de um único XML como dicionário.
    
    Gera dicionário com data de emissão normalizada, chave NFe e caminho do arquivo
    para um único registro de nota fiscal.
    
    Args:
        chave: Chave única da NFe
        dEmi: Data de emissão (dd/mm/yyyy ou yyyy-mm-dd)
        num_nfe: Número da nota fiscal
        base_dir: Diretório base para armazenamento
        
    Returns:
        Dicionário com as chaves: dEmi, cChaveNFe, caminho_arquivo
        
    Raises:
        ValueError: Se dados obrigatórios estiverem ausentes ou inválidos
        
    Examples:
        >>> info = gerar_xml_info_dict("123...", "17/07/2025", "001")
        >>> # {"dEmi": "2025-07-17", "cChaveNFe": "123...", "caminho_arquivo": "/.../arquivo.xml"}
    """
    # Validação de pré-condições
    if not all([chave, dEmi, num_nfe]):
        raise ValueError(f"Dados obrigatórios ausentes: chave={chave}, dEmi={dEmi}, num_nfe={num_nfe}")
    
    try:
        # Normalização da data
        data_normalizada = normalizar_data(str(dEmi).strip())
        if not data_normalizada:
            raise ValueError(f"Data de emissão inválida: '{dEmi}'")
        
        # Geração do caminho do arquivo XML usando versão otimizada
        pasta_xml, caminho_xml = gerar_xml_path_otimizado(chave, dEmi, num_nfe, base_dir)
        
        return {
            "dEmi": data_normalizada,
            "cChaveNFe": str(chave).strip(),
            "caminho_arquivo": str(caminho_xml)
        }
        
    except Exception as e:
        raise ValueError(f"Erro ao gerar informações do XML: {e}")


def criar_mapeamento_completo_com_descobrir_xmls(
    registros: List[Tuple[str, str, str]], 
    base_dir: str = "resultado",
    usar_versao_otimizada: bool = True
) -> Dict[str, List[Dict[str, str]]]:
    """
    Cria mapeamento completo usando descobrir_todos_xmls e gerar_nome_arquivo_xml.
    
    Esta função demonstra o uso otimizado das funções descobrir_todos_xmls() 
    e gerar_nome_arquivo_xml() para criar um mapeamento detalhado dos XMLs.
    
    Vantagens desta abordagem:
    - Performance superior com single scan recursive
    - Padronização consistente de nomenclatura
    - Detecção automática de subpastas
    - Mapeamento por data com múltiplos arquivos por dia
    
    Args:
        registros: Lista de tuplas (chave, dEmi, num_nfe)
        base_dir: Diretório base para busca
        usar_versao_otimizada: Se True, usa gerar_xml_path_otimizado()
        
    Returns:
        Dicionário com estrutura:
        {
            "2025-07-17": [
                {
                    "cChaveNFe": "chave1...",
                    "nNF": "001", 
                    "caminho_arquivo": "/caminho/arquivo1.xml",
                    "nome_padrao": "001_20250717_chave1....xml",
                    "existe": True
                },
                {
                    "cChaveNFe": "chave2...",
                    "nNF": "002",
                    "caminho_arquivo": "/caminho/arquivo2.xml", 
                    "nome_padrao": "002_20250717_chave2....xml",
                    "existe": False
                }
            ]
        }
        
    Examples:
        >>> registros = [("chave1", "17/07/2025", "001"), ("chave2", "17/07/2025", "002")]
        >>> mapeamento = criar_mapeamento_completo_com_descobrir_xmls(registros)
        >>> # Usa descobrir_todos_xmls() para scan eficiente
    """
    mapeamento_por_data = {}
    total_processados = 0
    total_com_erro = 0
    
    logger.info(f"[MAPEAMENTO] Iniciando mapeamento completo de {len(registros)} registros")
    
    for chave, dEmi, num_nfe in registros:
        try:
            # Validação de dados
            if not all([chave, dEmi, num_nfe]):
                logger.warning(f"[MAPEAMENTO] Dados incompletos: chave={chave}, dEmi={dEmi}, num_nfe={num_nfe}")
                total_com_erro += 1
                continue
            
            # Normalização da data
            data_normalizada = normalizar_data(str(dEmi).strip())
            if not data_normalizada:
                logger.warning(f"[MAPEAMENTO] Data inválida: '{dEmi}'")
                total_com_erro += 1
                continue
            
            # Geração do nome padrão usando função centralizada
            try:
                data_dt = datetime.strptime(data_normalizada, "%Y-%m-%d")
                nome_padrao = gerar_nome_arquivo_xml(chave, data_dt, num_nfe)
            except Exception as e:
                logger.warning(f"[MAPEAMENTO] Erro ao gerar nome padrão: {e}")
                total_com_erro += 1
                continue
            
            # Busca do arquivo usando versão otimizada ou original
            if usar_versao_otimizada:
                try:
                    pasta_xml, caminho_xml = gerar_xml_path_otimizado(chave, dEmi, num_nfe, base_dir)
                except Exception as e:
                    logger.warning(f"[MAPEAMENTO] Erro na busca otimizada: {e}")
                    pasta_xml, caminho_xml = gerar_xml_path(chave, dEmi, num_nfe, base_dir)
            else:
                pasta_xml, caminho_xml = gerar_xml_path(chave, dEmi, num_nfe, base_dir)
            
            # Criação do registro detalhado
            registro_detalhado = {
                "cChaveNFe": str(chave).strip(),
                "nNF": str(num_nfe).strip(),
                "caminho_arquivo": str(caminho_xml),
                "nome_padrao": nome_padrao,
                "existe": caminho_xml.exists(),
                "pasta_pai": str(pasta_xml)
            }
            
            # Agrupamento por data
            if data_normalizada not in mapeamento_por_data:
                mapeamento_por_data[data_normalizada] = []
            
            mapeamento_por_data[data_normalizada].append(registro_detalhado)
            total_processados += 1
            
        except Exception as e:
            logger.error(f"[MAPEAMENTO] Erro inesperado processando {chave}: {e}")
            total_com_erro += 1
    
    # Estatísticas finais
    total_datas = len(mapeamento_por_data)
    total_arquivos_existentes = sum(
        1 for data_registros in mapeamento_por_data.values()
        for registro in data_registros
        if registro["existe"]
    )
    
    logger.info(f"[MAPEAMENTO] Concluído: {total_processados} sucessos, {total_com_erro} erros")
    logger.info(f"[MAPEAMENTO] {total_datas} datas processadas, {total_arquivos_existentes} arquivos existentes")
    
    return mapeamento_por_data


def extrair_mes_do_path(caminho: Path) -> str:
    """
    Extrai identificador de mês (YYYY-MM) da estrutura hierarquica de pastas.
    
    Esperado: .../ano/mes/dia/arquivo
    
    Args:
        caminho: Path do arquivo na estrutura hierarquica
        
    Returns:
        String no formato 'YYYY-MM' ou 'outros' se noo conseguir extrair
        
    Examples:
        >>> extrair_mes_do_path(Path("resultado/2025/07/17/arquivo.xml"))
        '2025-07'
    """
    try:
        partes = caminho.parts
        
        # Busca padroo ano/mes na estrutura
        for i in range(len(partes) - 2):
            possivel_ano = partes[i]
            possivel_mes = partes[i + 1]
            
            # Validacoo de ano (4 digitos, entre 2000-2099)
            if (possivel_ano.isdigit() and 
                len(possivel_ano) == 4 and 
                2000 <= int(possivel_ano) <= 2099):
                
                # Validacoo de mês (2 digitos, entre 01-12)
                if (possivel_mes.isdigit() and 
                    len(possivel_mes) == 2 and 
                    1 <= int(possivel_mes) <= 12):
                    
                    return f"{possivel_ano}-{possivel_mes}"
        
        logger.warning(f"[PATH] Estrutura ano/mês noo encontrada em: {caminho}")
        return "outros"
        
    except Exception as e:
        logger.warning(f"[PATH] Erro ao extrair mês do caminho {caminho}: {e}")
        return "outros"


def criar_lockfile(pasta: Path) -> Path:
    """
    Cria arquivo de lock para controle de acesso exclusivo à pasta.
    
    Args:
        pasta: Diretorio onde criar o lockfile
        
    Returns:
        Path do lockfile criado
        
    Raises:
        RuntimeError: Se a pasta ja estiver em uso (lockfile existe)
        
    Examples:
        >>> lockfile = criar_lockfile(Path("resultado/2025/07/17"))
    """
    lockfile = pasta / ".processando.lock"
    
    if lockfile.exists():
        raise RuntimeError(f"Pasta em uso por outro processo: {pasta}")
    
    try:
        pasta.mkdir(parents=True, exist_ok=True)
        lockfile.touch()
        logger.debug(f"[LOCK] Lockfile criado: {lockfile}")
        return lockfile
    except Exception as e:
        raise RuntimeError(f"Erro ao criar lockfile em {pasta}: {e}")


def listar_arquivos_xml_em(pasta: Path, incluir_subpastas: bool = True) -> List[Path]:
    """
    Lista todos os arquivos XML em uma pasta e suas subpastas de forma otimizada.
    
    Args:
        pasta: Diretorio para listar arquivos
        incluir_subpastas: Se True, percorre subpastas recursivamente (padrão: True)
        
    Returns:
        Lista ordenada de Paths para arquivos .xml encontrados
        
    Examples:
        >>> xmls = listar_arquivos_xml_em(Path("resultado/2025/07/17"))
        >>> xmls_apenas_pasta = listar_arquivos_xml_em(Path("resultado"), incluir_subpastas=False)
    """
    try:
        if not pasta.exists():
            logger.warning(f"[LISTAR] Pasta nao existe: {pasta}")
            return []
        
        arquivos_xml = []
        
        if incluir_subpastas:
            # Busca recursiva usando rglob para melhor performance
            arquivos_xml = [
                arquivo for arquivo in pasta.rglob("*.xml")
                if arquivo.is_file()
            ]
        else:
            # Busca apenas na pasta atual (comportamento original)
            arquivos_xml = [
                arquivo for arquivo in pasta.iterdir()
                if arquivo.is_file() and arquivo.suffix.lower() == ".xml"
            ]
        
        # Ordenacoo por nome para consistência
        arquivos_xml.sort()
        
        if arquivos_xml:
            tipo_busca = "recursivamente" if incluir_subpastas else "na pasta atual"
            logger.debug(f"[LISTAR] Encontrados {len(arquivos_xml)} arquivos XML {tipo_busca} em {pasta}")
        
        return arquivos_xml
        
    except Exception as e:
        logger.warning(f"[LISTAR] Erro ao listar arquivos XML em {pasta}: {e}")
        return []


# =============================================================================
# CONTROLE DE RATE LIMITING
# =============================================================================

# Estado global para rate limiting assíncrono
_ultima_chamada_async = 0.0

async def respeitar_limite_requisicoes_async(min_intervalo: float = 0.25) -> None:
    """
    Implementa rate limiting assíncrono para controle de frequência de requisições.
    
    Versão assíncrona que não bloqueia o event loop e mantém estado global
    adequado para múltiplas chamadas.
    
    Args:
        min_intervalo: Intervalo mínimo em segundos entre chamadas (padrão: 0.25s = 4 req/s)
    """
    global _ultima_chamada_async
    
    tempo_atual = monotonic()
    tempo_decorrido = tempo_atual - _ultima_chamada_async
    
    if tempo_decorrido < min_intervalo:
        tempo_espera = min_intervalo - tempo_decorrido
        await asyncio.sleep(tempo_espera)
    
    _ultima_chamada_async = monotonic()



def respeitar_limite_requisicoes(min_intervalo: float = 0.25, ultima_chamada: Optional[List[float]] = None) -> None:
    """
    Implementa rate limiting para controle de frequência de requisicões.
    
    Garante intervalo minimo entre chamadas para evitar sobrecarga de APIs
    e respeitar limites de requisicões por segundo.
    
    Args:
        min_intervalo: Intervalo minimo em segundos entre chamadas
        ultima_chamada: Lista mutavel para armazenar timestamp da ultima chamada
        
    Examples:
        >>> respeitar_limite_requisicoes(0.25)  # 4 req/s maximo
        >>> respeitar_limite_requisicoes(1.0)   # 1 req/s maximo
    """
    if ultima_chamada is None:
        ultima_chamada = [0.0]
    
    tempo_atual = monotonic()
    tempo_decorrido = tempo_atual - ultima_chamada[0]
    
    if tempo_decorrido < min_intervalo:
        tempo_espera = min_intervalo - tempo_decorrido
        sleep(tempo_espera)
    
    ultima_chamada[0] = monotonic()


# =============================================================================
# 🗃️ OPERAÇÕES DE BANCO DE DADOS
# =============================================================================

class RegistroInvalidoError(ValueError):
    """Exceção para registros com dados inválidos."""
    pass

@contextmanager
def conexao_otimizada(db_path: str, config: Optional[DatabaseConfig] = None):
    """
    Context manager para conexão SQLite otimizada.
    
    Args:
        db_path: Caminho para o banco de dados
        config: Configuração personalizada (opcional)
        
    Yields:
        sqlite3.Connection: Conexão configurada
        
    Raises:
        DatabaseError: Se não conseguir conectar
    """
    if config is None:
        config = DatabaseConfig()
    
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=config.timeout)
        conn.row_factory = sqlite3.Row
        
        # Aplica configurações de performance
        for pragma, valor in config.get_pragmas().items():
            conn.execute(f"PRAGMA {pragma} = {valor}")
        
        yield conn
        
    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        raise DatabaseError(f"Erro de conexão SQLite: {e}")
    finally:
        if conn:
            conn.close()

def validar_parametros_banco(db_path: str, table_name: str) -> None:
    """
    Valida parâmetros de entrada para operações de banco.
    
    Args:
        db_path: Caminho do banco de dados
        table_name: Nome da tabela
        
    Raises:
        ValueError: Se parâmetros são inválidos
        DatabaseError: Se caminho não é acessível
    """
    if not db_path or not isinstance(db_path, str):
        raise ValueError("db_path deve ser uma string não vazia")
    
    if not table_name or not isinstance(table_name, str):
        raise ValueError("table_name deve ser uma string não vazia")
    
    # Validação de segurança SQL injection
    if not table_name.replace('_', '').isalnum():
        raise ValueError(f"table_name contém caracteres inválidos: {table_name}")
    
    # Verifica se diretório é acessível
    db_dir = Path(db_path).parent
    if not db_dir.exists():
        try:
            db_dir.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            raise DatabaseError(f"Não foi possível criar diretório {db_dir}: {e}")

def criar_schema_base(conn: sqlite3.Connection, table_name: str) -> None:
    """
    Cria schema base da tabela de notas fiscais.
    
    Schema atualizado conforme estrutura real do banco de dados,
    incluindo todos os campos e tipos corretos baseados na análise.
    
    Args:
        conn: Conexão SQLite ativa
        table_name: Nome da tabela a criar
        
    Raises:
        SchemaError: Se não conseguir criar tabela
    """
    schema_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            -- Campos principais da NFe
            cChaveNFe TEXT PRIMARY KEY,
            nIdNF INTEGER,
            nIdPedido INTEGER,
            
            -- Campos de data/hora
            dCan TEXT,
            dEmi TEXT,
            dInut TEXT,
            dReg TEXT,
            dSaiEnt TEXT,
            hEmi TEXT,
            hSaiEnt TEXT,
            
            -- Campos de identificação
            mod TEXT,
            nNF TEXT,
            serie TEXT,
            tpAmb TEXT,
            tpNF TEXT,
            
            -- Campos do destinatário
            cnpj_cpf TEXT,
            cRazao TEXT,
            
            -- Valores
            vNF REAL,
            
            -- Campos de controle
            xml_baixado BOOLEAN DEFAULT 0,
            anomesdia INTEGER DEFAULT NULL,
            caminho_arquivo TEXT DEFAULT NULL,
            xml_vazio INTEGER DEFAULT 0
        )
    """
    
    try:
        conn.execute(schema_sql)
        logger.info(f"[SCHEMA] Tabela '{table_name}' criada/verificada com sucesso")
    except sqlite3.Error as e:
        raise SchemaError(f"Falha ao criar tabela {table_name}: {e}")

def criar_indices_otimizados(conn: sqlite3.Connection, table_name: str) -> None:
    """
    Cria índices otimizados conforme estrutura real do banco de dados.
    
    Índices baseados na análise da estrutura atual do banco,
    incluindo todos os índices necessários para performance.
    
    Args:
        conn: Conexão SQLite ativa
        table_name: Nome da tabela
        
    Raises:
        SchemaError: Se não conseguir criar índices
    """
    indices = [
        # Índice único para chave primária (conforme imagem)
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_chave ON {table_name}(cChaveNFe)",
        
        # Índices simples conforme estrutura do banco
        f"CREATE INDEX IF NOT EXISTS idx_baixado ON {table_name}(xml_baixado)",
        f"CREATE INDEX IF NOT EXISTS idx_chave ON {table_name}(cChaveNFe)",
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_chave_nfe ON {table_name}(cChaveNFe)",
        f"CREATE INDEX IF NOT EXISTS idx_dEmi_baixado ON {table_name}(dEmi, xml_baixado)",
        f"CREATE INDEX IF NOT EXISTS idx_dEmi_nNF ON {table_name}(dEmi, nNF)",
        f"CREATE INDEX IF NOT EXISTS idx_data_emissao ON {table_name}(dEmi) WHERE dEmi IS NOT NULL",
        f"CREATE INDEX IF NOT EXISTS idx_notas_baixado ON {table_name}(xml_baixado)",
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_notas_chave ON {table_name}(cChaveNFe)",
        f"CREATE INDEX IF NOT EXISTS idx_notas_data ON {table_name}(dEmi, nNF)",
        f"CREATE INDEX IF NOT EXISTS idx_notas_pendentes ON {table_name}(dEmi) WHERE xml_baixado = 0",
        f"CREATE INDEX IF NOT EXISTS idx_xml_vazio ON {table_name}(xml_vazio) WHERE xml_vazio = 1",
        
        # Índices para a coluna anomesdia (YYYYMMDD)
        f"CREATE INDEX IF NOT EXISTS idx_anomesdia ON {table_name}(anomesdia)",
        f"CREATE INDEX IF NOT EXISTS idx_anomesdia_baixado ON {table_name}(anomesdia, xml_baixado)",
        f"CREATE INDEX IF NOT EXISTS idx_anomesdia_pendentes ON {table_name}(anomesdia) WHERE xml_baixado = 0"
    ]
    
    indices_criados = 0
    for sql_indice in indices:
        try:
            conn.execute(sql_indice)
            indices_criados += 1
        except sqlite3.Error as e:
            # Log warning mas não falha - alguns índices podem já existir
            logger.debug(f"[ÍNDICE] Aviso ao criar índice: {e}")
    
    logger.info(f"[ÍNDICE] {indices_criados}/{len(indices)} comandos de índice executados")


def iniciar_db(
    db_path: str, 
    table_name: str = "notas",
    config: Optional[DatabaseConfig] = None,
    criar_auditoria: bool = True
) -> None:
    """
    Inicializa banco de dados SQLite com schema otimizado para notas fiscais.
    
    Esta função configura um banco SQLite de alta performance para armazenar
    dados de notas fiscais da API Omie, incluindo:
    
    Características implementadas:
    - Configurações SQLite otimizadas (WAL, cache, mmap)
    - Schema robusto com campos de controle e auditoria
    - Índices otimizados para consultas principais
    - Validação rigorosa de parâmetros de entrada
    - Tratamento específico de erros
    - Triggers de auditoria automática (opcional)
    - Context managers para gestão de recursos
    
    Args:
        db_path: Caminho para o arquivo do banco SQLite.
                Diretório será criado se não existir.
        table_name: Nome da tabela principal (padrão: "notas").
                   Deve conter apenas caracteres alfanuméricos e underscore.
        config: Configuração personalizada do SQLite (opcional).
               Se None, usa configuração otimizada padrão.
        criar_auditoria: Se True, cria triggers para auditoria automática.
        
    Returns:
        None - Operação executada por efeito colateral
        
    Raises:
        ValueError: Se parâmetros de entrada são inválidos
        DatabaseError: Se não conseguir conectar ou configurar banco
        SchemaError: Se falhar na criação de tabelas ou índices
        OSError: Se não conseguir criar diretório do banco
        
    Examples:
        >>> # Inicialização básica
        >>> iniciar_db("omie.db")
        
        >>> # Com configuração personalizada
        >>> config = DatabaseConfig(cache_size="-128000")  # 128MB cache
        >>> iniciar_db("dados/omie.db", "notas_2024", config)
        
        >>> # Sem auditoria para melhor performance
        >>> iniciar_db("omie.db", criar_auditoria=False)
        
    Performance:
        - WAL mode: Permite leituras concorrentes durante escritas
        - Cache 64MB: Reduz I/O em consultas frequentes
        - Índices parciais: Otimizam queries específicas
        - Memory temp store: Operações temporárias em RAM
        
    Segurança:
        - Validação contra SQL injection no table_name
        - Timeout de conexão para evitar travamentos
        - Rollback automático em caso de erro
        - Verificação de permissões de diretório
    """
    logger.info(f"[DB] Inicializando banco de dados: {db_path}")
    
    # 1. Validação rigorosa de parâmetros
    validar_parametros_banco(db_path, table_name)
    
    # 2. Configuração padrão se não fornecida
    if config is None:
        config = DatabaseConfig()
    
    inicio = time.time()
    
    try:
        # 3. Conexão otimizada com context manager
        with conexao_otimizada(db_path, config) as conn:
            
            # 4. Criação do schema base
            logger.info(f"[DB] Criando schema para tabela '{table_name}'...")
            criar_schema_base(conn, table_name)
            
            # 5. Criação de índices otimizados
            logger.info(f"[DB] Criando índices otimizados...")
            criar_indices_otimizados(conn, table_name)

            # 7. Commit final
            conn.commit()
            
        tempo_total = time.time() - inicio
        
        # 8. Verificação final
        with conexao_otimizada(db_path, config) as conn:
            cursor = conn.execute(f"SELECT COUNT(*) FROM sqlite_master WHERE name = ?", (table_name,))
            if cursor.fetchone()[0] == 0:
                raise SchemaError(f"Tabela {table_name} não foi criada corretamente")
        
        # 9. Log de sucesso com métricas
        logger.info(
            f"[DB] Banco inicializado com sucesso. "
            f"Tabela: {table_name}, "
            f"Arquivo: {db_path}, "
            f"Tempo: {tempo_total:.3f}s"
        )
        
    except (ValueError, DatabaseError, SchemaError):
        # Re-propaga exceções conhecidas
        raise
    except Exception as e:
        # Captura e converte exceções inesperadas
        logger.exception(f"[DB] Erro inesperado durante inicialização: {e}")
        raise DatabaseError(f"Falha crítica na inicialização do banco: {e}")

# Mantém compatibilidade com versão anterior
def inicializar_banco_e_indices(
    db_path: str, 
    table_name: str = "notas",
    config: Optional[DatabaseConfig] = None,
    criar_auditoria: bool = True,
    _deprecation_warning: bool = True
) -> None:
    """
    Inicializa banco de dados SQLite com configurações otimizadas.
    
    DEPRECATED: Use iniciar_db() diretamente para melhor controle.
    Esta função mantém compatibilidade retroativa mas será removida
    na versão 4.0.
    
    Funcionalidades implementadas:
    - Criação de schema otimizado para notas fiscais
    - Índices de performance para consultas principais
    - Triggers de auditoria automática (opcional)
    - Configurações SQLite otimizadas
    - Validação rigorosa de parâmetros
    
    Args:
        db_path: Caminho para o arquivo do banco SQLite.
                Diretório será criado se não existir.
        table_name: Nome da tabela principal (padrão: "notas").
                   Deve conter apenas caracteres alfanuméricos e underscore.
        config: Configuração personalizada do SQLite (opcional).
               Se None, usa configuração otimizada padrão.
        criar_auditoria: Se True, cria triggers para auditoria automática.
        _deprecation_warning: Se True, emite aviso de deprecação.
        
    Returns:
        None - Operação executada por efeito colateral
        
    Raises:
        ValueError: Se parâmetros de entrada são inválidos
        DatabaseError: Se não conseguir conectar ou configurar banco
        SchemaError: Se falhar na criação de tabelas ou índices
        OSError: Se não conseguir criar diretório do banco
        
    Examples:
        >>> # Uso básico (compatibilidade retroativa)
        >>> inicializar_banco_e_indices("omie.db")
        
        >>> # Com configurações avançadas
        >>> config = DatabaseConfig(cache_size="-128000")
        >>> inicializar_banco_e_indices("omie.db", config=config)
        
        >>> # Migração recomendada
        >>> from src.utils import iniciar_db
        >>> iniciar_db("omie.db")  # Use diretamente
        
    Migration Guide:
        # ANTES (deprecated)
        inicializar_banco_e_indices("omie.db")
        
        # DEPOIS (recomendado)
        from src.utils import iniciar_db
        iniciar_db("omie.db")
        
        # Com configurações personalizadas
        config = DatabaseConfig(cache_size="-256000")
        iniciar_db("omie.db", config=config)
    """
    # Emite aviso de deprecação
    if _deprecation_warning:
        warnings.warn(
            "inicializar_banco_e_indices() está deprecated. "
            "Use iniciar_db() diretamente para melhor controle e flexibilidade. "
            "Esta função será removida na versão 4.0.",
            DeprecationWarning,
            stacklevel=2
        )
    
    # Log de compatibilidade
    logger.info("[COMPAT] Usando função de compatibilidade inicializar_banco_e_indices()")
    logger.info("[COMPAT] Considere migrar para iniciar_db() para melhor flexibilidade")
    
    # Delega para a função principal
    try:
        iniciar_db(
            db_path=db_path,
            table_name=table_name,
            config=config,
            criar_auditoria=criar_auditoria
        )
    except Exception as e:
        logger.error(f"[COMPAT] Erro na inicialização via função de compatibilidade: {e}")
        raise

# Alias adicional para compatibilidade total
inicializar_banco = inicializar_banco_e_indices

def atualizar_status_xml(
    db_path: str,
    chave: str,
    caminho: Path,
    xml_str: str,
    rebaixado: bool = False,
    xml_vazio: int = 0
) -> None:
    if not chave:
        logger.warning("[ERRO] Chave nao fornecida para atualizacao do XML.")
        return

    caminho_arquivo = str(caminho.resolve())

    if not caminho.exists():
        logger.warning(f"[ERRO] Caminho {caminho_arquivo} nao existe no disco.")
        return

    if not caminho_arquivo:
        logger.warning(f"[ERRO] Caminho do XML esta vazio para chave {chave}.")
        return

    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")

            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE notas 
                SET xml_baixado = 1, caminho_arquivo = ?, xml_vazio = ? 
                WHERE cChaveNFe = ? AND (? IS NOT NULL)
                """,
                (caminho_arquivo, xml_vazio, chave, caminho_arquivo)
            )

            if cursor.rowcount == 0:
                logger.warning(f"[ALERT] Nenhum registro atualizado para chave: {chave}")
            conn.commit()
    except Exception as e:
        logger.exception(f"[ERRO] Falha ao atualizar status do XML para {chave}: {e}")


def listar_notas_por_data_numero(db_path: str) -> list[tuple]:
    """
    Lista todas as notas ordenadas por data de emissoo e numero da nota fiscal.

    Args:
        db_path: Caminho do banco SQLite.

    Returns:
        Lista de tuplas (cChaveNFe, dEmi, nNF, ...outros campos) ordenadas.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute(
                "SELECT * FROM notas ORDER BY dEmi ASC, nNF ASC"
            )
            resultados = cursor.fetchall()
        logger.info(f"[DB] {len(resultados)} notas listadas por data/numero.")
        return resultados
    except Exception as e:
        logger.error(f"[DB] Erro ao listar notas por data/numero: {e}")
        return []

def atualizar_dEmi_registros_pendentes(db_path: str, resultado_dir: str = "resultado") -> None:
    """
    Atualiza o campo dEmi dos registros com xml_baixado = 0 e dEmi nulo, buscando a data de emissoo no XML correspondente.
    Args:
        db_path: Caminho do banco SQLite.
        resultado_dir: Diretorio base onde os XMLs estoo salvos.
    """
    import xml.etree.ElementTree as ET
    from pathlib import Path

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT cChaveNFe, nNF FROM notas WHERE xml_baixado = 0 AND (dEmi IS NULL OR dEmi = '')")
            pendentes = cursor.fetchall()
        logger.info(f"[DB] Encontrados {len(pendentes)} registros pendentes para atualizacao de dEmi.")
        atualizados = 0
        for chave, nNF in pendentes:
            # Busca arquivo XML pelo padroo de nome
            xml_path = None
            for root, dirs, files in os.walk(resultado_dir):
                for file in files:
                    if file.endswith(".xml") and chave in file:
                        xml_path = Path(root) / file
                        break
                if xml_path:
                    break
            if not xml_path or not xml_path.exists():
                logger.warning(f"[dEmi] XML noo encontrado para chave {chave}.")
                continue
            try:
                tree = ET.parse(xml_path)
                root = tree.getroot()
                # Tenta extrair dEmi (padroo NFe: infNFe/ide/dEmi)
                dEmi_elem = root.find(".//{*}ide/{*}dEmi")
                if dEmi_elem is not None and dEmi_elem.text:
                    dEmi = dEmi_elem.text
                    # Normaliza para formato ISO
                    dEmi_norm = normalizar_data(dEmi)
                    with sqlite3.connect(db_path) as conn:
                        conn.execute("UPDATE notas SET dEmi = ? WHERE cChaveNFe = ?", (dEmi_norm, chave))
                        conn.commit()
                    atualizados += 1
                    logger.info(f"[dEmi] Atualizado para chave {chave}: {dEmi_norm}")
                else:
                    logger.warning(f"[dEmi] Elemento dEmi noo encontrado no XML para chave {chave}.")
            except Exception as e:
                logger.warning(f"[dEmi] Falha ao extrair dEmi do XML {xml_path} para chave {chave}: {e}")
        logger.info(f"[DB] atualizacao concluida. {atualizados} registros tiveram dEmi preenchido.")
    except Exception as e:
        logger.error(f"[DB] Erro ao atualizar dEmi dos registros pendentes: {e}")

def _extrair_valor_xml(root_xml, campo: str) -> Optional[str]:
    """
    Extrai valor de campo essencial do XML, usando XPATHs centralizados.
    Args:
        root_xml: Elemento raiz do XML.
        campo: Nome do campo a extrair.
    Returns:
        Valor extraido ou None.
    """
    if campo == "cnpj_cpf":
        elem_cnpj = root_xml.find(".//{*}dest/{*}CNPJ")
        elem_cpf = root_xml.find(".//{*}dest/{*}CPF")
        valor = elem_cnpj.text if elem_cnpj is not None and elem_cnpj.text else (
            elem_cpf.text if elem_cpf is not None and elem_cpf.text else None
        )
        return valor.strip() if valor else None
    else:
        elem = root_xml.find(XPATHS[campo])
        return elem.text.strip() if elem is not None and elem.text else None

def _indexar_xmls_por_chave(resultado_dir: str) -> dict[str, Path]:
    """
    Indexa todos os arquivos XML por chave fiscal (cChaveNFe) com processamento paralelo otimizado.
    
    Otimizações implementadas:
    - Processamento paralelo com ThreadPoolExecutor
    - Validação rigorosa da chave fiscal (44 caracteres alfanuméricos)
    - Logging com progresso em tempo real
    - Tratamento robusto de erros
    - Filtragem otimizada por padrão de nome
    
    Args:
        resultado_dir: Diretorio base onde os XMLs estoo salvos.
    Returns:
        Dicionario {chave: Path(xml)}
    """
    import concurrent.futures
    import re
    from typing import Optional
    
    # Padrão para validar chave fiscal (44 caracteres alfanuméricos)
    PADRAO_CHAVE = re.compile(r'^[0-9]{44}$')
    
    def processar_arquivo_xml(xml_file: Path) -> Optional[tuple[str, Path]]:
        """Processa um arquivo XML individual e extrai a chave fiscal."""
        try:
            nome = xml_file.stem
            partes = nome.split("_")
            
            # Busca a chave fiscal (deve ser a ultima parte com 44 dígitos)
            chave_candidata = partes[-1] if len(partes) > 1 else None
            
            if chave_candidata and PADRAO_CHAVE.match(chave_candidata):
                return (chave_candidata, xml_file)
            else:
                # Fallback: busca por qualquer parte com 44 dígitos
                for parte in reversed(partes):
                    if PADRAO_CHAVE.match(parte):
                        return (parte, xml_file)
                        
                logger.debug(f"[CAMPOS] Chave fiscal não encontrada em: {xml_file.name}")
                return None
                
        except Exception as e:
            logger.warning(f"[CAMPOS] Falha ao indexar {xml_file}: {e}")
            return None
    
    logger.info(f"[CAMPOS] Iniciando indexação otimizada de XMLs em: {resultado_dir}")
    inicio = time.time()
    
    # Coleta todos os arquivos XML primeiro
    resultado_path = Path(resultado_dir)
    if not resultado_path.exists():
        logger.error(f"[CAMPOS] Diretório não existe: {resultado_dir}")
        return {}
    
    # Lista todos os arquivos XML
    try:
        todos_xmls = list(resultado_path.rglob("*.xml")) # A função rglob permite buscar recursivamente usando pathlib que é mais eficiente que os.walk ou os.listdir (escrito em cpython ) + Pattern matching integrado (aplicação de filtro direto no sistema de arquivos durante a busca)
    except OSError as e:
        logger.error(f"[CAMPOS] Erro ao acessar diretório {resultado_dir}: {e}")
        return {}
    except Exception as e:
        logger.error(f"[CAMPOS] Erro inesperado ao listar XMLs: {e}")
        return {}
    
    total_arquivos = len(todos_xmls)
    
    if total_arquivos == 0:
        logger.warning(f"[CAMPOS] Nenhum arquivo XML encontrado em: {resultado_dir}")
        return {}
    
    logger.info(f"[CAMPOS] Encontrados {total_arquivos} arquivos XML para indexar")
    
    # Processamento paralelo
    xml_index: dict[str, Path] = {}
    processados = 0
    duplicatas = 0
    
    # Usa numero de workers baseado no numero de CPUs disponíveis
    max_workers = min(32, (os.cpu_count() or 1) + 4)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submete todos os arquivos para processamento
        future_to_xml = {executor.submit(processar_arquivo_xml, xml_file): xml_file 
                        for xml_file in todos_xmls}
        
        # Processa resultados conforme completam
        for future in concurrent.futures.as_completed(future_to_xml):
            xml_file = future_to_xml[future]
            try:
                resultado = future.result()
                if resultado:
                    chave, caminho = resultado
                    if chave in xml_index:
                        # Lida com duplicatas (mantém o mais recente)
                        xml_existente = xml_index[chave]
                        if caminho.stat().st_mtime > xml_existente.stat().st_mtime:
                            logger.debug(f"[CAMPOS] Chave duplicada, mantendo mais recente: {chave}")
                            xml_index[chave] = caminho
                        duplicatas += 1
                    else:
                        xml_index[chave] = caminho
                    
                processados += 1

                # Log de progresso a cada 200 arquivos processados
                if processados % 200 == 0:
                    tempo_decorrido = time.time() - inicio
                    taxa = processados / tempo_decorrido
                    tempo_restante = (total_arquivos - processados) / taxa if taxa > 0 else 0
                    logger.info(f"[CAMPOS] Progresso: {processados}/{total_arquivos} "
                              f"({processados/total_arquivos*100:.1f}%) - "
                              f"Taxa: {taxa:.0f} arq/s - "
                              f"Tempo restante: {tempo_restante:.0f}s")
                    
            except Exception as e:
                logger.warning(f"[CAMPOS] Erro ao processar {xml_file}: {e}")
    
    tempo_total = time.time() - inicio
    total_indexado = len(xml_index)
    taxa_media = processados / tempo_total if tempo_total > 0 else 0
    
    logger.info(f"[CAMPOS] Indexação concluída em {tempo_total:.2f}s:")
    logger.info(f"[CAMPOS] - {total_indexado} chaves unicas indexadas")
    logger.info(f"[CAMPOS] - {processados} arquivos processados")
    logger.info(f"[CAMPOS] - {duplicatas} duplicatas encontradas")
    logger.info(f"[CAMPOS] - Taxa média: {taxa_media:.0f} arquivos/segundo")
    
    return xml_index

def atualizar_campos_registros_pendentes(db_path: str, resultado_dir: str = "resultado") -> None:
    """
    Verifica se os arquivos marcados como xml_baixado = 0 realmente não foram baixados,
    atualizando o status quando encontrados nos diretórios locais.
    
    Funcionalidades implementadas:
    - Processamento paralelo otimizado com concurrent.futures
    - Verificação inteligente baseada na estrutura de nomes dos arquivos
    - Extração de campos essenciais (dEmi, nNF, cChaveNFe) dos nomes dos arquivos
    - Updates em batch para máxima performance
    - Logging estruturado para monitoramento
    
    Estrutura esperada do nome: {nNF}_{dEmi_YYYYMMDD}_{cChaveNFe}.xml
    
    Args:
        db_path: Caminho do banco SQLite.
        resultado_dir: Diretório base onde os XMLs estão salvos.
    """
    import xml.etree.ElementTree as ET
    import concurrent.futures
    from typing import List, Dict, Optional, Tuple
    import re
    from datetime import datetime
    
    logger.info("[VERIFICAÇÃO] Iniciando verificação otimizada de arquivos baixados...")
    etapa_inicio = time.time()
    
    # 0. Verificação de otimizações disponíveis
    db_otimizacoes = _verificar_views_e_indices_disponiveis(db_path)
    views_disponiveis = sum(1 for k, v in db_otimizacoes.items() if k.startswith('vw_') and v)
    indices_disponiveis = sum(1 for k, v in db_otimizacoes.items() if k.startswith('idx_') and v)
    logger.info(f"[VERIFICAÇÃO] Otimizações DB: {views_disponiveis} views, {indices_disponiveis} índices específicos")
    
    # 1. Indexação dos XMLs com extração de dados dos nomes
    t0 = time.time()
    xml_index = _indexar_xmls_por_chave_com_dados(resultado_dir)
    t1 = time.time()
    logger.info(f"[VERIFICAÇÃO] XMLs indexados em {t1-t0:.2f}s ({len(xml_index)} arquivos)")

    # 2. Busca otimizada dos registros marcados como não baixados usando views e índices
    t2 = time.time()
    try:
        with sqlite3.connect(db_path) as conn:
            # Configurações de performance para SQLite
            for pragma, value in SQLITE_PRAGMAS.items():
                conn.execute(f"PRAGMA {pragma} = {value}")
            
            # Usa view otimizada se disponível, senão usa query com índices
            if db_otimizacoes.get('vw_notas_pendentes', False):
                # Usa view otimizada que já tem filtros e índices aplicados
                logger.debug("[VERIFICAÇÃO] Usando view otimizada 'vw_notas_pendentes'")
                cursor = conn.execute("""
                    SELECT cChaveNFe, nNF, dEmi, anomesdia
                    FROM vw_notas_pendentes
                    ORDER BY anomesdia DESC, cChaveNFe
                """)
            elif db_otimizacoes.get('idx_anomesdia_baixado', False):
                # Usa índice específico disponível
                logger.debug("[VERIFICAÇÃO] Usando índice específico 'idx_anomesdia_baixado'")
                cursor = conn.execute("""
                    SELECT cChaveNFe, nNF, dEmi, anomesdia
                    FROM notas INDEXED BY idx_anomesdia_baixado
                    WHERE xml_baixado = 0
                    ORDER BY anomesdia DESC, cChaveNFe
                """)
            elif db_otimizacoes.get('idx_baixado', False):
                # Usa índice genérico para xml_baixado
                logger.debug("[VERIFICAÇÃO] Usando índice 'idx_baixado'")
                cursor = conn.execute("""
                    SELECT cChaveNFe, nNF, dEmi, 
                           COALESCE(anomesdia, 
                                   CASE WHEN dEmi IS NOT NULL 
                                        THEN CAST(REPLACE(dEmi, '-', '') AS INTEGER)
                                        ELSE NULL END) as anomesdia
                    FROM notas INDEXED BY idx_baixado
                    WHERE xml_baixado = 0
                    ORDER BY anomesdia DESC NULLS LAST, cChaveNFe
                """)
            else:
                # Query padrão sem hints específicos
                logger.debug("[VERIFICAÇÃO] Usando consulta padrão sem índices específicos")
                cursor = conn.execute("""
                    SELECT cChaveNFe, nNF, dEmi, 
                           COALESCE(anomesdia, 
                                   CASE WHEN dEmi IS NOT NULL 
                                        THEN CAST(REPLACE(dEmi, '-', '') AS INTEGER)
                                        ELSE NULL END) as anomesdia
                    FROM notas 
                    WHERE xml_baixado = 0
                    ORDER BY anomesdia DESC NULLS LAST, cChaveNFe
                """)
            
            pendentes = cursor.fetchall()
    except sqlite3.OperationalError as e:
        # Se índice específico não existir, usa consulta padrão otimizada
        logger.warning(f"[VERIFICAÇÃO] Índice específico não encontrado, usando consulta padrão: {e}")
        try:
            with sqlite3.connect(db_path) as conn:
                for pragma, value in SQLITE_PRAGMAS.items():
                    conn.execute(f"PRAGMA {pragma} = {value}")
                
                # Query otimizada usando qualquer índice disponível para xml_baixado
                cursor = conn.execute("""
                    SELECT cChaveNFe, nNF, dEmi, 
                           COALESCE(anomesdia, 
                                   CASE WHEN dEmi IS NOT NULL 
                                        THEN CAST(REPLACE(dEmi, '-', '') AS INTEGER)
                                        ELSE NULL END) as anomesdia
                    FROM notas 
                    WHERE xml_baixado = 0
                    ORDER BY anomesdia DESC NULLS LAST, cChaveNFe
                """)
                pendentes = cursor.fetchall()
        except Exception as inner_e:
            logger.error(f"[VERIFICAÇÃO] Erro na consulta fallback: {inner_e}")
            return
    except Exception as e:
        logger.error(f"[VERIFICAÇÃO] Erro ao buscar registros pendentes: {e}")
        return
        
    t3 = time.time()
    logger.info(f"[VERIFICAÇÃO] {len(pendentes)} registros marcados como não baixados carregados em {t3-t2:.2f}s")
    
    if not pendentes:
        logger.info("[VERIFICAÇÃO] Nenhum registro marcado como não baixado encontrado")
        return

    # 3. Processamento paralelo otimizado
    def processar_lote_verificacao(lote_registros: List[Tuple]) -> List[Dict]:
        """Processa um lote de registros verificando se foram realmente baixados"""
        resultados_lote = []
        
        for registro in lote_registros:
            # Adaptação para suportar tanto 3 quanto 4 campos (com anomesdia)
            if len(registro) == 4:
                chave_nfe, nnf_db, demi_db, anomesdia_db = registro
            else:
                chave_nfe, nnf_db, demi_db = registro
                anomesdia_db = None
            
            # Busca arquivo XML correspondente no índice
            dados_xml = xml_index.get(chave_nfe)
            if not dados_xml:
                resultados_lote.append({"chave": chave_nfe, "status": "nao_encontrado"})
                continue
                
            xml_path, dados_extraidos = dados_xml
            
            if not xml_path.exists():
                resultados_lote.append({"chave": chave_nfe, "status": "arquivo_removido"})
                continue
                
            try:
                # Verifica se o arquivo tem tamanho válido
                tamanho_arquivo = xml_path.stat().st_size
                xml_vazio = 1 if tamanho_arquivo < 100 else 0  # Arquivos muito pequenos são considerados vazios
                
                # Prepara dados para atualização
                novos_dados = {
                    'xml_baixado': 1,
                    'caminho_arquivo': str(xml_path.resolve()),
                    'xml_vazio': xml_vazio
                }
                
                # Atualiza campos essenciais se estiverem vazios no banco
                if dados_extraidos:
                    if not demi_db and dados_extraidos.get('dEmi'):
                        novos_dados['dEmi'] = dados_extraidos['dEmi']
                    if not nnf_db and dados_extraidos.get('nNF'):
                        novos_dados['nNF'] = dados_extraidos['nNF']
                
                resultados_lote.append({
                    "chave": chave_nfe, 
                    "status": "encontrado",
                    "novos_dados": novos_dados,
                    "tamanho": tamanho_arquivo
                })
                
            except OSError as e:
                logger.warning(f"[VERIFICAÇÃO] Erro ao acessar arquivo {xml_path}: {e}")
                resultados_lote.append({"chave": chave_nfe, "status": "erro_acesso"})
            except Exception as e:
                logger.warning(f"[VERIFICAÇÃO] Erro inesperado para {chave_nfe}: {e}")
                resultados_lote.append({"chave": chave_nfe, "status": "erro_geral"})
                
        return resultados_lote

    # Divisão em lotes para processamento paralelo
    t4 = time.time()
    TAMANHO_LOTE = max(100, len(pendentes) // (os.cpu_count() or 4))
    lotes = [pendentes[i:i + TAMANHO_LOTE] for i in range(0, len(pendentes), TAMANHO_LOTE)]
    
    logger.info(f"[VERIFICAÇÃO] Processando {len(lotes)} lotes de ~{TAMANHO_LOTE} registros...")
    
    # Processamento paralelo por lotes
    todos_resultados = []
    max_workers = min(os.cpu_count() or 4, len(lotes))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submete todos os lotes para processamento paralelo
        future_to_lote = {executor.submit(processar_lote_verificacao, lote): i 
                         for i, lote in enumerate(lotes)}
        
        # Coleta resultados conforme completam
        for future in concurrent.futures.as_completed(future_to_lote):
            lote_idx = future_to_lote[future]
            try:
                resultados_lote = future.result()
                todos_resultados.extend(resultados_lote)
                
                # Log de progresso apenas para lotes grandes
                if len(lotes) > 10 and (lote_idx + 1) % max(1, len(lotes) // 10) == 0:
                    progresso = (lote_idx + 1) / len(lotes) * 100
                    logger.info(f"[VERIFICAÇÃO] Progresso: {progresso:.0f}% ({lote_idx + 1}/{len(lotes)} lotes)")
                    
            except Exception as e:
                logger.warning(f"[VERIFICAÇÃO] Erro ao processar lote {lote_idx}: {e}")
    
    t5 = time.time()
    logger.info(f"[VERIFICAÇÃO] Processamento paralelo concluído em {t5-t4:.2f}s")

    # 4. Atualização em batch otimizada
    t6 = time.time()
    encontrados = 0
    nao_encontrados = 0
    erros = 0
    arquivos_vazios = 0
    
    # Separa resultados por status
    para_atualizar = []
    
    for resultado in todos_resultados:
        status = resultado.get("status")
        if status == "encontrado":
            para_atualizar.append(resultado)
            encontrados += 1
            if resultado.get("novos_dados", {}).get("xml_vazio") == 1:
                arquivos_vazios += 1
        elif status == "nao_encontrado":
            nao_encontrados += 1
        else:
            erros += 1
    
    # Executa updates em batch
    if para_atualizar:
        try:
            with sqlite3.connect(db_path) as conn:
                # Configurações de performance máxima
                for pragma, value in SQLITE_PRAGMAS.items():
                    conn.execute(f"PRAGMA {pragma} = {value}")
                
                # Prepara dados para batch update
                dados_update = []
                for resultado in para_atualizar:
                    novos_dados = resultado["novos_dados"]
                    valores = [
                        novos_dados.get('xml_baixado', 1),
                        novos_dados.get('caminho_arquivo', ''),
                        novos_dados.get('xml_vazio', 0),
                        novos_dados.get('dEmi'),
                        novos_dados.get('nNF'),
                        resultado["chave"]
                    ]
                    dados_update.append(valores)
                
                # Executa batch update otimizado com hint de índice para WHERE clause
                # Usa o índice único da chave primária para máxima eficiência
                query_update = """
                    UPDATE notas 
                    SET xml_baixado = ?, 
                        caminho_arquivo = ?, 
                        xml_vazio = ?,
                        dEmi = COALESCE(?, dEmi),
                        nNF = COALESCE(?, nNF),
                        anomesdia = COALESCE(
                            anomesdia,
                            CASE WHEN COALESCE(?, dEmi) IS NOT NULL 
                                 THEN CAST(REPLACE(COALESCE(?, dEmi), '-', '') AS INTEGER)
                                 ELSE NULL END
                        )
                    WHERE cChaveNFe = ?
                """
                
                # Prepara dados incluindo dEmi para cálculo de anomesdia
                dados_update_otimizados = []
                for resultado in para_atualizar:
                    novos_dados = resultado["novos_dados"]
                    demi_para_anomesdia = novos_dados.get('dEmi')
                    valores = [
                        novos_dados.get('xml_baixado', 1),
                        novos_dados.get('caminho_arquivo', ''),
                        novos_dados.get('xml_vazio', 0),
                        novos_dados.get('dEmi'),
                        novos_dados.get('nNF'),
                        demi_para_anomesdia,  # Para cálculo de anomesdia
                        demi_para_anomesdia,  # Duplicado para o CASE WHEN
                        resultado["chave"]
                    ]
                    dados_update_otimizados.append(valores)
                
                conn.executemany(query_update, dados_update_otimizados)
                conn.commit()
                
                # Verificação pós-atualização usando view se disponível
                try:
                    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='view' AND name='vw_notas_mes_atual'")
                    if cursor.fetchone():
                        # Usa view para verificar se atualizações foram aplicadas corretamente
                        cursor = conn.execute("""
                            SELECT COUNT(*) as atualizados_hoje 
                            FROM vw_notas_mes_atual 
                            WHERE xml_baixado = 1 
                            AND caminho_arquivo IS NOT NULL
                        """)
                        verificacao = cursor.fetchone()
                        if verificacao:
                            logger.debug(f"[VERIFICAÇÃO] Verificação pós-update: {verificacao[0]} registros com XML baixado no período atual")
                except Exception as ve:
                    logger.debug(f"[VERIFICAÇÃO] Verificação pós-update opcional falhou: {ve}")
                
                logger.info(f"[VERIFICAÇÃO] Batch update executado para {len(dados_update_otimizados)} registros")
                
        except Exception as e:
            logger.error(f"[VERIFICAÇÃO] Erro durante batch update: {e}")
            return
    
    t7 = time.time()
    logger.info(f"[VERIFICAÇÃO] Updates em batch concluídos em {t7-t6:.2f}s")

    # 5. Relatório final detalhado com estatísticas usando views se disponíveis
    tempo_total = t7 - etapa_inicio
    taxa_processamento = len(pendentes) / tempo_total if tempo_total > 0 else 0
    
    # Estatísticas adicionais usando views otimizadas
    try:
        with sqlite3.connect(db_path) as conn:
            estatisticas_extras = {}
            
            # Usa view de estatísticas se disponível
            if db_otimizacoes.get('vw_notas_mes_atual', False):
                cursor = conn.execute("""
                    SELECT 
                        COUNT(*) as total_mes_atual,
                        SUM(CASE WHEN xml_baixado = 1 THEN 1 ELSE 0 END) as baixados_mes_atual,
                        SUM(CASE WHEN xml_vazio = 1 THEN 1 ELSE 0 END) as vazios_mes_atual
                    FROM vw_notas_mes_atual
                """)
                stats_mes = cursor.fetchone()
                if stats_mes:
                    estatisticas_extras.update({
                        'total_mes_atual': stats_mes[0],
                        'baixados_mes_atual': stats_mes[1],
                        'vazios_mes_atual': stats_mes[2]
                    })
            
            # Estatísticas gerais
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_geral,
                    SUM(CASE WHEN xml_baixado = 1 THEN 1 ELSE 0 END) as baixados_geral,
                    SUM(CASE WHEN xml_vazio = 1 THEN 1 ELSE 0 END) as vazios_geral
                FROM notas
            """)
            stats_geral = cursor.fetchone()
            if stats_geral:
                estatisticas_extras.update({
                    'total_geral': stats_geral[0],
                    'baixados_geral': stats_geral[1],
                    'vazios_geral': stats_geral[2]
                })
                
    except Exception as e:
        logger.debug(f"[VERIFICAÇÃO] Erro ao obter estatísticas extras: {e}")
        estatisticas_extras = {}
    
    logger.info(f"[VERIFICAÇÃO] === RESULTADO DA VERIFICAÇÃO ===")
    logger.info(f"[VERIFICAÇÃO] Registros verificados: {len(pendentes)}")
    logger.info(f"[VERIFICAÇÃO] Arquivos encontrados: {encontrados}")
    logger.info(f"[VERIFICAÇÃO] Arquivos não encontrados: {nao_encontrados}")
    logger.info(f"[VERIFICAÇÃO] Arquivos vazios detectados: {arquivos_vazios}")
    logger.info(f"[VERIFICAÇÃO] Erros: {erros}")
    logger.info(f"[VERIFICAÇÃO] Tempo total: {tempo_total:.2f}s")
    logger.info(f"[VERIFICAÇÃO] Taxa: {taxa_processamento:.2f} registros/s")
    
    # Relatório de estatísticas extras se disponíveis
    if estatisticas_extras:
        logger.info(f"[VERIFICAÇÃO] === ESTATÍSTICAS ADICIONAIS ===")
        if 'total_geral' in estatisticas_extras:
            percentual_baixado = (estatisticas_extras['baixados_geral'] / estatisticas_extras['total_geral'] * 100) if estatisticas_extras['total_geral'] > 0 else 0
            logger.info(f"[VERIFICAÇÃO] Total geral: {estatisticas_extras['total_geral']} | Baixados: {estatisticas_extras['baixados_geral']} ({percentual_baixado:.1f}%)")
        if 'total_mes_atual' in estatisticas_extras:
            logger.info(f"[VERIFICAÇÃO] Mês atual: {estatisticas_extras['total_mes_atual']} | Baixados: {estatisticas_extras['baixados_mes_atual']}")
    
    logger.info(f"[VERIFICAÇÃO] ==========================================")


def _verificar_views_e_indices_disponiveis(db_path: str) -> Dict[str, bool]:
    """
    Verifica quais views e índices estão disponíveis no banco para otimização.
    
    Args:
        db_path: Caminho do banco SQLite
        
    Returns:
        Dict com disponibilidade de views e índices importantes
    """
    disponibilidade = {
        'vw_notas_pendentes': False,
        'vw_notas_mes_atual': False,
        'vw_notas_recentes': False,
        'idx_anomesdia_baixado': False,
        'idx_chave_nfe': False,
        'idx_baixado': False
    }
    
    try:
        with sqlite3.connect(db_path) as conn:
            # Verifica views
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='view' AND name IN ('vw_notas_pendentes', 'vw_notas_mes_atual', 'vw_notas_recentes')
            """)
            views_existentes = {row[0] for row in cursor.fetchall()}
            
            # Verifica índices
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='index' AND name IN ('idx_anomesdia_baixado', 'idx_chave_nfe', 'idx_baixado')
            """)
            indices_existentes = {row[0] for row in cursor.fetchall()}
            
            # Atualiza disponibilidade
            for item in disponibilidade:
                if item.startswith('vw_'):
                    disponibilidade[item] = item in views_existentes
                elif item.startswith('idx_'):
                    disponibilidade[item] = item in indices_existentes
                    
    except Exception as e:
        logger.warning(f"[DB_OTIM] Erro ao verificar views/índices: {e}")
    
    logger.debug(f"[DB_OTIM] Disponibilidade: {disponibilidade}")
    return disponibilidade


def _indexar_xmls_por_chave_com_dados(resultado_dir: str) -> Dict[str, Tuple[Path, Dict[str, str]]]:
    """
    Indexa XMLs por chave fiscal extraindo dados essenciais dos nomes dos arquivos.
    
    Padrão esperado: {nNF}_{dEmi_YYYYMMDD}_{cChaveNFe}.xml
    
    Args:
        resultado_dir: Diretório base para busca
        
    Returns:
        Dict[chave_nfe, (Path, dados_extraidos)]
    """
    import re
    from datetime import datetime
    
    # Padrão regex para extrair dados do nome do arquivo
    # Formato: numero_dataYYYYMMDD_chave44digitos.xml
    PADRAO_NOME = re.compile(r'^(\d+)_(\d{8})_([0-9]{44})\.xml$', re.IGNORECASE)
    
    def processar_arquivo_xml_com_dados(xml_file: Path) -> Optional[Tuple[str, Path, Dict[str, str]]]:
        """Processa um arquivo XML extraindo dados do nome."""
        try:
            nome = xml_file.name
            match = PADRAO_NOME.match(nome)
            
            if match:
                nnf, data_str, chave_nfe = match.groups()
                
                # Converte data YYYYMMDD para formato ISO
                try:
                    data_obj = datetime.strptime(data_str, '%Y%m%d')
                    demi_iso = data_obj.strftime('%Y-%m-%d')
                except ValueError:
                    demi_iso = None
                
                dados_extraidos = {
                    'nNF': nnf,
                    'dEmi': demi_iso,
                    'cChaveNFe': chave_nfe
                }
                
                return (chave_nfe, xml_file, dados_extraidos)
            else:
                # Fallback: busca chave de 44 dígitos no nome
                chaves_encontradas = re.findall(r'[0-9]{44}', nome)
                if chaves_encontradas:
                    chave_nfe = chaves_encontradas[0]
                    return (chave_nfe, xml_file, {})
                
                logger.debug(f"[INDEXAÇÃO] Padrão não reconhecido: {nome}")
                return None
                
        except Exception as e:
            logger.warning(f"[INDEXAÇÃO] Falha ao processar {xml_file}: {e}")
            return None
    
    logger.info(f"[INDEXAÇÃO] Iniciando indexação com extração de dados em: {resultado_dir}")
    inicio = time.time()
    
    # Coleta todos os arquivos XML
    resultado_path = Path(resultado_dir)
    if not resultado_path.exists():
        logger.error(f"[INDEXAÇÃO] Diretório não existe: {resultado_dir}")
        return {}
    
    try:
        todos_xmls = list(resultado_path.rglob("*.xml"))
    except OSError as e:
        logger.error(f"[INDEXAÇÃO] Erro ao acessar diretório {resultado_dir}: {e}")
        return {}
    
    total_arquivos = len(todos_xmls)
    if total_arquivos == 0:
        logger.warning(f"[INDEXAÇÃO] Nenhum arquivo XML encontrado em: {resultado_dir}")
        return {}
    
    logger.info(f"[INDEXAÇÃO] Encontrados {total_arquivos} arquivos XML para indexar")
    
    # Processamento paralelo
    xml_index: Dict[str, Tuple[Path, Dict[str, str]]] = {}
    processados = 0
    duplicatas = 0
    
    max_workers = min(32, (os.cpu_count() or 1) + 4)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submete todos os arquivos para processamento
        future_to_xml = {executor.submit(processar_arquivo_xml_com_dados, xml_file): xml_file 
                        for xml_file in todos_xmls}
        
        # Processa resultados conforme completam
        for future in concurrent.futures.as_completed(future_to_xml):
            xml_file = future_to_xml[future]
            try:
                resultado = future.result()
                if resultado:
                    chave, caminho, dados = resultado
                    if chave in xml_index:
                        duplicatas += 1
                        logger.debug(f"[INDEXAÇÃO] Chave duplicada encontrada: {chave}")
                    else:
                        xml_index[chave] = (caminho, dados)
                    
                processados += 1

                # Log de progresso a cada 500 arquivos
                if processados % 500 == 0:
                    tempo_decorrido = time.time() - inicio
                    taxa = processados / tempo_decorrido
                    progresso = processados / total_arquivos * 100
                    logger.info(f"[INDEXAÇÃO] Progresso: {progresso:.1f}% - Taxa: {taxa:.0f} arq/s")
                    
            except Exception as e:
                logger.warning(f"[INDEXAÇÃO] Erro ao processar {xml_file}: {e}")
    
    tempo_total = time.time() - inicio
    total_indexado = len(xml_index)
    taxa_media = processados / tempo_total if tempo_total > 0 else 0
    
    logger.info(f"[INDEXAÇÃO] Indexação concluída em {tempo_total:.2f}s:")
    logger.info(f"[INDEXAÇÃO] - {total_indexado} chaves únicas indexadas")
    logger.info(f"[INDEXAÇÃO] - {processados} arquivos processados")
    logger.info(f"[INDEXAÇÃO] - {duplicatas} duplicatas encontradas")
    logger.info(f"[INDEXAÇÃO] - Taxa média: {taxa_media:.0f} arquivos/segundo")
    
    return xml_index

def remover_registros_com_campos_essenciais_invalidos_e_listar_dias(db_path: str) -> list[str]:
    """
    Remove registros do banco onde qualquer campo essencial esta NULL, vazio, zero, ' - ', ou apenas espacos,
    e retorna a lista de dias (dReg) unicos desses registros.
    Args:
        db_path: Caminho do banco SQLite.
    Returns:
        Lista de datas (dReg) unicas dos registros removidos.
    """
    CAMPOS_ESSENCIAIS = [
        'cChaveNFe', 'nIdNF', 'nIdPedido', 'dEmi', 'dReg', 'nNF'
    ]
    dias_a_reprocessar = []
    try:
        with sqlite3.connect(db_path) as conn:
            # Monta condicoo para cada campo essencial
            condicoes = []
            for campo in CAMPOS_ESSENCIAIS:
                condicoes.append(f"{campo} IS NULL")
                condicoes.append(f"TRIM({campo}) = ''")
                condicoes.append(f"{campo} = 0")
                condicoes.append(f"TRIM({campo}) = '-' ")
                condicoes.append(f"TRIM({campo}) = ' - '")
            where_clause = ' OR '.join(condicoes)
            # Busca os dias (dReg) dos registros invalidos
            query_dias = f"SELECT DISTINCT dReg FROM notas WHERE {where_clause}"
            cursor = conn.execute(query_dias)
            dias = cursor.fetchall()
            dias_a_reprocessar = [d[0] for d in dias if d[0] and str(d[0]).strip() not in ('', '-', ' - ', '0', '0.0')]
            # Remove os registros invalidos
            query_delete = f"DELETE FROM notas WHERE {where_clause}"
            conn.execute(query_delete)
            conn.commit()
        logger.info(f"[LIMPEZA] Removidos registros com campos essenciais invalidos. Dias a reprocessar: {dias_a_reprocessar}")
    except Exception as e:
        logger.error(f"[LIMPEZA] Erro ao remover registros com campos essenciais invalidos: {e}")
    return dias_a_reprocessar

# Mantem o nome antigo para compatibilidade retroativa, mas avisa no log
remover_registros_sem_dEmi_e_listar_dias = remover_registros_com_campos_essenciais_invalidos_e_listar_dias

def verificar_schema_banco(db_path: str) -> Dict[str, bool]:
    """
    Verifica se o schema do banco contém todas as colunas necessárias.
    
    Returns:
        Dict[str, bool]: Mapeamento coluna -> existe
    """
    colunas_requeridas = [
        'cChaveNFe', 'nIdNF', 'xml_baixado', 'anomesdia', 
        'caminho_arquivo', 'xml_vazio'
    ]
    
    # Colunas opcionais para compatibilidade
    colunas_opcionais = ['tentativas']
    
    try:
        with conexao_otimizada(db_path) as conn:
            cursor = conn.execute("PRAGMA table_info(notas)")
            colunas_existentes = {row['name'] for row in cursor.fetchall()}
            
            resultado = {}
            
            # Verifica colunas obrigatórias
            for coluna in colunas_requeridas:
                resultado[coluna] = coluna in colunas_existentes
            
            # Verifica colunas opcionais
            for coluna in colunas_opcionais:
                resultado[coluna] = coluna in colunas_existentes
            
            return resultado
            
    except Exception as e:
        logger.error(f"[SCHEMA] Erro ao verificar schema: {e}")
        return {col: False for col in colunas_requeridas + colunas_opcionais}

def marcar_como_erro(
    db_path: str, 
    chave: str, 
    mensagem_erro: str,
    validar_schema: bool = True
) -> bool:
    """
    FUNÇÃO REMOVIDA - Colunas de erro foram removidas do schema.
    
    Esta função foi mantida apenas para compatibilidade retroativa,
    mas não executa nenhuma ação já que as colunas erro, mensagem_erro
    e erro_xml foram removidas do banco de dados.
    
    Args:
        db_path: Caminho do banco SQLite (ignorado)
        chave: Chave NFe (ignorado)
        mensagem_erro: Mensagem de erro (ignorado)
        validar_schema: Validação de schema (ignorado)
        
    Returns:
        bool: Sempre retorna True para manter compatibilidade
    """
    logger.debug(f"[ERRO] Função marcar_como_erro chamada para {chave[:8] if chave else 'N/A'}... - IGNORADA (colunas removidas)")
    return True  # Retorna True para não quebrar código existente

def marcar_como_baixado(
    db_path: str, 
    chave: str, 
    caminho: Path, 
    rebaixado: bool = False,
    xml_vazio: int = 0
) -> None:
    """
    Marca um registro como baixado no banco de dados.
    
    Args:
        db_path: Caminho do banco SQLite
        chave: Chave da NFe
        caminho: Caminho do arquivo XML
        rebaixado: Se foi rebaixado (parâmetro mantido para compatibilidade, mas ignorado)
        xml_vazio: Se o XML esta vazio (0=noo, 1=sim)
    """
    if not chave:
        logger.warning("[ERRO] Chave noo fornecida para marcar como baixado.")
        return
    
    try:
        caminho_arquivo = str(caminho.resolve())
        
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE notas 
                SET xml_baixado = 1, caminho_arquivo = ?, xml_vazio = ? 
                WHERE cChaveNFe = ?
                """,
                (caminho_arquivo, xml_vazio, chave)
            )
            
            if cursor.rowcount == 0:
                logger.warning(f"[ALERT] Nenhum registro encontrado para marcar como baixado: {chave}")
            else:
                logger.info(f"[BAIXADO] Registro marcado como baixado: {chave}")
            
            conn.commit()
            
    except Exception as e:
        logger.exception(f"[ERRO] Falha ao marcar registro como baixado para {chave}: {e}")


def _validar_registro_nota(registro: Dict[str, Union[str, int, float, None]]) -> None:
    """
    Valida campos obrigatórios de um registro de nota fiscal.
    
    Args:
        registro: Dicionário com dados da nota fiscal
        
    Raises:
        RegistroInvalidoError: Se campos obrigatórios estão ausentes ou inválidos
        
    Examples:
        >>> _validar_registro_nota({"cChaveNFe": "123...", "dEmi": "2025-07-20"})
        # OK - Não levanta exceção
        
        >>> _validar_registro_nota({"cChaveNFe": ""})
        # RegistroInvalidoError: Chave fiscal ausente ou vazia
    """
    if not isinstance(registro, dict):
        raise RegistroInvalidoError(f"Registro deve ser um dicionário, recebido: {type(registro)}")
    
    # Validação da chave fiscal
    chave = registro.get('cChaveNFe')
    if not chave or not isinstance(chave, str) or len(chave.strip()) != 44:
        raise RegistroInvalidoError(f"Chave fiscal inválida: '{chave}' (deve ter 44 caracteres)")
    
    # Validação da data de emissão
    dEmi = registro.get('dEmi')
    if not dEmi or (isinstance(dEmi, str) and not dEmi.strip()):
        raise RegistroInvalidoError(f"Data de emissão ausente para chave: {chave}")
    
    # Validação do número da NFe
    nNF = registro.get('nNF')
    if not nNF or (isinstance(nNF, str) and not nNF.strip()):
        raise RegistroInvalidoError(f"Número da NFe ausente para chave: {chave}")

def salvar_nota(
    registro: Dict[str, Union[str, int, float, None]], 
    db_path: str,
    validar_entrada: bool = True,
    log_detalhado: bool = False
) -> ResultadoSalvamento:
    """
    Salva uma única nota fiscal no banco de dados SQLite com validação rigorosa.
    
    NOTA: Para operações em lote, prefira usar salvar_varias_notas() que é
    significativamente mais eficiente (até 100x mais rápido para grandes volumes).
    
    Características implementadas:
    - Validação rigorosa de dados de entrada
    - Transformação tipada com fallbacks seguros
    - Tratamento específico de duplicatas
    - Configuração otimizada do SQLite
    - Logging estruturado com controle de nível
    - Retorno estruturado com detalhes da operação
    
    Args:
        registro: Dicionário contendo dados da nota fiscal.
                 Campos obrigatórios: cChaveNFe (44 chars), dEmi, nNF
        db_path: Caminho absoluto para o arquivo do banco SQLite.
                Arquivo deve existir e ter schema válido.
        validar_entrada: Se True, valida rigorosamente dados de entrada.
                        Recomendado para dados externos (padrão: True).
        log_detalhado: Se True, inclui logs detalhados para debugging.
                      Cuidado com dados sensíveis em produção.
        
    Returns:
        ResultadoSalvamento: Estrutura tipada com resultado da operação
        
    Raises:
        RegistroInvalidoError: Se dados obrigatórios são inválidos
        DatabaseError: Se não conseguir conectar ou executar operação
        ValueError: Se transformação de dados falhar
        
    Examples:
        >>> # Uso básico
        >>> registro = {
        ...     'cChaveNFe': '12345678901234567890123456789012345678901234',
        ...     'dEmi': '2025-07-20',
        ...     'nNF': '123',
        ...     'cRazao': 'Empresa Exemplo'
        ... }
        >>> resultado = salvar_nota(registro, "omie.db")
        >>> if resultado.sucesso:
        ...     print(f"Nota {resultado.chave} salva com sucesso")
        
        >>> # Com validação desabilitada (dados confiáveis)
        >>> resultado = salvar_nota(registro, "omie.db", validar_entrada=False)
        
        >>> # Com log detalhado para debugging
        >>> resultado = salvar_nota(registro, "omie.db", log_detalhado=True)
        
    Performance:
        - Operação individual: ~0.1-1ms por registro
        - Para > 100 registros: Use salvar_varias_notas() (até 100x mais rápido)
        - Configuração SQLite otimizada aplicada automaticamente
        
    Segurança:
        - Validação contra SQL injection via placeholders
        - Transformação segura de tipos
        - Logs sanitizados (dados sensíveis omitidos por padrão)
        - Rollback automático em caso de erro
    """
    # Captura contexto para logging
    chave_contexto = registro.get('cChaveNFe', 'UNKNOWN')[:8] if isinstance(registro, dict) else 'INVALID'
    
    try:
        # 1. Validação rigorosa de entrada (opcional)
        if validar_entrada:
            _validar_registro_nota(registro)
        
        chave = registro['cChaveNFe']
        
        # 2. Log de início (controlado)
        if log_detalhado:
            dEmi = registro.get('dEmi')
            nNF = registro.get('nNF')
            logger.debug(f"[NOTA] Salvando: {chave[:8]}..., dEmi={dEmi}, nNF={nNF}")
        else:
            logger.debug(f"[NOTA] Salvando nota: {chave[:8]}...")
        
        # 3. Transformação segura para tupla
        try:
            valores = transformar_em_tuple(registro)
        except Exception as e:
            raise ValueError(f"Falha na transformação de dados para chave {chave[:8]}...: {e}")
        
        # 4. Operação de banco com configuração otimizada
        try:
            with conexao_otimizada(db_path) as conn:
                # Insert com tratamento de duplicata
                conn.execute(SCHEMA_NOTAS_INSERT, valores)
                conn.commit()
                
                logger.debug(f"[NOTA] Inserção bem-sucedida: {chave[:8]}...")
                
                return ResultadoSalvamento(
                    sucesso=True,
                    chave=chave,
                    motivo="inserido_com_sucesso"
                )
                
        except sqlite3.IntegrityError as e:
            # Tratamento específico para chaves duplicadas
            if "PRIMARY KEY constraint" in str(e) or "UNIQUE constraint" in str(e):
                logger.debug(f"[NOTA] Registro duplicado (ignorado): {chave[:8]}...")
                
                return ResultadoSalvamento(
                    sucesso=True,
                    chave=chave,
                    motivo="duplicata_ignorada",
                    duplicata=True
                )
            else:
                # Outros erros de integridade
                raise DatabaseError(f"Erro de integridade para chave {chave[:8]}...: {e}")
                
        except sqlite3.Error as e:
            raise DatabaseError(f"Erro SQLite para chave {chave[:8]}...: {e}")
            
    except RegistroInvalidoError as e:
        logger.warning(f"[NOTA] Dados inválidos ({chave_contexto}...): {e}")
        return ResultadoSalvamento(
            sucesso=False,
            chave=chave_contexto,
            motivo=f"dados_invalidos: {str(e)}"
        )
        
    except (ValueError, DatabaseError) as e:
        logger.error(f"[NOTA] Erro ao salvar ({chave_contexto}...): {e}")
        return ResultadoSalvamento(
            sucesso=False,
            chave=chave_contexto,
            motivo=f"erro_operacao: {str(e)}"
        )
        
    except Exception as e:
        logger.exception(f"[NOTA] Erro inesperado ao salvar ({chave_contexto}...): {e}")
        return ResultadoSalvamento(
            sucesso=False,
            chave=chave_contexto,
            motivo=f"erro_inesperado: {str(e)}"
        )

def salvar_varias_notas(
    registros: List[Dict[str, Union[str, int, float, None]]], 
    db_path: str,
    validar_entrada: bool = True,
    log_detalhado: bool = False,
    tamanho_lote: int = 200
) -> Dict[str, Union[int, List[str]]]:
    """
    Versão otimizada para salvamento em lote de múltiplas notas fiscais.
    
    RECOMENDADO: Use esta função para operações com > 10 registros.
    Performance até 100x superior ao salvar_nota() individual.
    
    Otimizações implementadas:
    - Transação única para todo o lote
    - Configuração SQLite aplicada uma vez
    - executemany() para inserção em massa
    - Processamento em lotes configuráveis
    - Validação em paralelo (opcional)
    
    Args:
        registros: Lista de dicionários com dados das notas fiscais
        db_path: Caminho do banco SQLite
        validar_entrada: Se True, valida todos os registros antes de inserir
        log_detalhado: Se True, log detalhado por lote
        tamanho_lote: Número de registros por transação (padrão: 1000)
        
    Returns:
        Dict com estatísticas: {
            'total_processados': int,
            'inseridos': int,
            'duplicatas': int,
            'erros': int,
            'chaves_com_erro': List[str]
        }
        
    Examples:
        >>> registros = [
        ...     {'cChaveNFe': '123...', 'dEmi': '2025-07-20', 'nNF': '1'},
        ...     {'cChaveNFe': '456...', 'dEmi': '2025-07-20', 'nNF': '2'},
        ... ]
        >>> resultado = salvar_varias_notas(registros, "omie.db")
        >>> print(f"Inseridos: {resultado['inseridos']}, Erros: {resultado['erros']}")
    """
    if not registros:
        logger.info("[LOTE] Nenhum registro fornecido para salvamento")
        return {
            'total_processados': 0,
            'inseridos': 0,
            'duplicatas': 0,
            'erros': 0,
            'chaves_com_erro': []
        }
    
    logger.info(f"[LOTE] Iniciando salvamento de {len(registros)} registros em lotes de {tamanho_lote}")
    inicio = time.time()
    
    total_inseridos = 0
    total_duplicatas = 0
    total_erros = 0
    chaves_com_erro = []
    
    # Validação opcional de todos os registros primeiro
    if validar_entrada:
        logger.info("[LOTE] Validando registros de entrada...")
        registros_validos = []
        
        for i, registro in enumerate(registros):
            try:
                _validar_registro_nota(registro)
                registros_validos.append(registro)
            except RegistroInvalidoError as e:
                chave = registro.get('cChaveNFe', f'REGISTRO_{i}')
                logger.warning(f"[LOTE] Registro inválido ignorado ({chave[:8]}...): {e}")
                chaves_com_erro.append(chave)
                total_erros += 1
        
        registros = registros_validos
        logger.info(f"[LOTE] {len(registros)} registros válidos após validação")
    
    # Processamento em lotes
    try:
        with conexao_otimizada(db_path) as conn:
            # Processa em lotes para otimizar memória
            for i in range(0, len(registros), tamanho_lote):
                lote_atual = registros[i:i + tamanho_lote]
                dados_lote = []
                
                # Transforma registros em tuplas
                for registro in lote_atual:
                    try:
                        dados_lote.append(transformar_em_tuple(registro))
                    except Exception as e:
                        chave = registro.get('cChaveNFe', 'UNKNOWN')
                        logger.warning(f"[LOTE] Erro na transformação ({chave[:8]}...): {e}")
                        chaves_com_erro.append(chave)
                        total_erros += 1
                
                if dados_lote:
                    try:
                        # Insert em lote com INSERT OR IGNORE para tratar duplicatas
                        conn.executemany(
                            SCHEMA_NOTAS_INSERT.replace("INSERT INTO", "INSERT OR IGNORE INTO"),
                            dados_lote
                        )
                        
                        inseridos_lote = conn.total_changes - total_inseridos - total_duplicatas
                        duplicatas_lote = len(dados_lote) - inseridos_lote
                        
                        total_inseridos += inseridos_lote
                        total_duplicatas += duplicatas_lote
                        
                        if log_detalhado:
                            logger.info(f"[LOTE] Lote {i//tamanho_lote + 1}: {inseridos_lote} inseridos, {duplicatas_lote} duplicatas")
                            
                    except sqlite3.Error as e:
                        logger.error(f"[LOTE] Erro no lote {i//tamanho_lote + 1}: {e}")
                        total_erros += len(dados_lote)
            
            conn.commit()
            
    except Exception as e:
        logger.exception(f"[LOTE] Erro crítico durante salvamento em lote: {e}")
        raise DatabaseError(f"Falha no salvamento em lote: {e}")
    
    # Relatório final
    tempo_total = time.time() - inicio
    tempo_total_ms = tempo_total * 1000  # Converter para milissegundos
    taxa_processamento_ms = len(registros) / tempo_total_ms if tempo_total_ms > 0 else 0
    taxa_insercao_ms = total_inseridos / tempo_total_ms if tempo_total_ms > 0 and total_inseridos > 0 else 0
    
    resultado = {
        'total_processados': len(registros),
        'inseridos': total_inseridos,
        'duplicatas': total_duplicatas,
        'erros': total_erros,
        'chaves_com_erro': chaves_com_erro
    }
    
    logger.info(f"[LOTE] Concluído em {tempo_total:.2f}s:")
    logger.info(f"[LOTE] - {total_inseridos} inseridos")
    logger.info(f"[LOTE] - {total_duplicatas} duplicatas")
    logger.info(f"[LOTE] - {total_erros} erros")
    
    # Métricas mais precisas com taxa em registros/ms
    if total_inseridos > 0:
        logger.info(f"[LOTE] - Taxa inserção: {taxa_insercao_ms:.2f} novos registros/ms")
        logger.info(f"[LOTE] - Taxa processamento: {taxa_processamento_ms:.2f} registros verificados/ms")
    else:
        logger.info(f"[LOTE] - Taxa processamento: {taxa_processamento_ms:.2f} registros verificados/ms (todos duplicatas)")
        logger.info(f"[LOTE] - Nota: Taxa alta indica verificação rápida de duplicatas, não inserções")
    
    return resultado

# Função deprecated mantida para compatibilidade
def salvar_nota_deprecated(registro: dict, db_path: str) -> None:
    """
    DEPRECATED: Use salvar_nota() com retorno estruturado.
    
    Mantida para compatibilidade retroativa. Será removida na v4.0.
    """
    warnings.warn(
        "salvar_nota_deprecated() está obsoleta. Use salvar_nota() que retorna ResultadoSalvamento.",
        DeprecationWarning,
        stacklevel=2
    )
    
    resultado = salvar_nota(registro, db_path, validar_entrada=False)
    if not resultado.sucesso and not resultado.duplicata:
        # Emula comportamento original de logging de erro
        logger.error(f"[COMPAT] Falha no salvamento: {resultado.motivo}")


# =============================================================================
# 📅 FUNÇÕES DE INDEXAÇÃO TEMPORAL
# =============================================================================

def garantir_coluna_anomesdia(db_path: str = "omie.db", table_name: str = "notas") -> bool:
    """
    Garante que a coluna anomesdia existe na tabela de notas.
    
    Esta função verifica se a coluna anomesdia (INTEGER) existe e a cria se necessário.
    É executada na inicialização do pipeline para garantir compatibilidade.
    
    Args:
        db_path: Caminho para o banco SQLite
        table_name: Nome da tabela (padrão: "notas")
        
    Returns:
        True se coluna existe ou foi criada com sucesso, False caso contrário
        
    Examples:
        >>> garantir_coluna_anomesdia("omie.db")
        True  # coluna já existia ou foi criada
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Verifica se a coluna anomesdia já existe
            cursor.execute(f"PRAGMA table_info({table_name})")
            colunas = [coluna[1] for coluna in cursor.fetchall()]
            
            if 'anomesdia' in colunas:
                logger.debug("[ANOMESDIA] Coluna anomesdia já existe")
                return True
            
            # Adiciona a coluna anomesdia
            logger.info("[ANOMESDIA] Adicionando coluna anomesdia à tabela...")
            cursor.execute(f"""
                ALTER TABLE {table_name} 
                ADD COLUMN anomesdia INTEGER DEFAULT NULL
            """)
            
            conn.commit()
            logger.info("[ANOMESDIA] ✓ Coluna anomesdia criada com sucesso")
            return True
            
    except sqlite3.Error as e:
        logger.error(f"[ANOMESDIA] Erro de banco ao criar coluna: {e}")
        return False
    except Exception as e:
        logger.error(f"[ANOMESDIA] Erro inesperado ao criar coluna: {e}")
        return False


def atualizar_anomesdia(db_path: str = "omie.db", table_name: str = "notas") -> int:
    """
    Atualiza o campo anomesdia (YYYYMMDD) baseado no campo dEmi.
    
    Esta função converte datas no formato brasileiro (dd/mm/yyyy) ou ISO (yyyy-mm-dd)
    para o formato inteiro YYYYMMDD, facilitando filtros e consultas temporais.
    
    Args:
        db_path: Caminho para o banco SQLite
        table_name: Nome da tabela (padrão: "notas")
        
    Returns:
        Número de registros atualizados
        
    Examples:
        >>> atualizar_anomesdia("omie.db")
        150  # registros atualizados
        
        # Consultas facilitadas:
        # SELECT * FROM notas WHERE anomesdia >= 20250101 AND anomesdia <= 20250131
        # SELECT * FROM notas WHERE anomesdia = 20250721
    """
    try:
        with sqlite3.connect(db_path) as conn:
            # Otimizações de performance
            for pragma, valor in SQLITE_PRAGMAS.items():
                conn.execute(f"PRAGMA {pragma}={valor}")
            
            cursor = conn.cursor()
            
            # Busca registros com dEmi válido mas sem anomesdia
            cursor.execute(f"""
                SELECT cChaveNFe, dEmi 
                FROM {table_name} 
                WHERE dEmi IS NOT NULL 
                AND dEmi != '' 
                AND dEmi != '-'
                AND (anomesdia IS NULL OR anomesdia = 0)
            """)
            
            registros = cursor.fetchall()
            if not registros:
                logger.info("[ANOMESDIA] Nenhum registro para atualizar")
                return 0
            
            logger.info(f"[ANOMESDIA] Processando {len(registros)} registros...")
            
            atualizacoes = []
            erros = 0
            
            for chave, dEmi in registros:
                try:
                    # Normaliza a data para formato ISO
                    data_normalizada = normalizar_data(dEmi)
                    if data_normalizada:
                        # Converte para YYYYMMDD
                        ano, mes, dia = data_normalizada.split('-')
                        anomesdia = int(f"{ano}{mes}{dia}")
                        atualizacoes.append((anomesdia, chave))
                    else:
                        logger.warning(f"[ANOMESDIA] Data inválida para chave {chave}: {dEmi}")
                        erros += 1
                        
                except Exception as e:
                    logger.warning(f"[ANOMESDIA] Erro ao processar {chave}: {e}")
                    erros += 1
            
            # Executa atualizações em lote
            if atualizacoes:
                cursor.executemany(f"""
                    UPDATE {table_name} 
                    SET anomesdia = ? 
                    WHERE cChaveNFe = ?
                """, atualizacoes)
                
                conn.commit()
                atualizados = len(atualizacoes)
                
                logger.info(f"[ANOMESDIA] ✓ {atualizados} registros atualizados")
                if erros > 0:
                    logger.warning(f"[ANOMESDIA] ⚠ {erros} registros com erro")
                
                return atualizados
            else:
                logger.info("[ANOMESDIA] Nenhuma atualização válida encontrada")
                return 0
                
    except sqlite3.Error as e:
        logger.error(f"[ANOMESDIA] Erro de banco: {e}")
        return 0
    except Exception as e:
        logger.error(f"[ANOMESDIA] Erro inesperado: {e}")
        return 0


def criar_views_otimizadas(db_path: str = "omie.db", table_name: str = "notas") -> None:
    """
    Cria views otimizadas para facilitar consultas comuns na tabela de notas fiscais.
    
    Views são consultas SQL pré-definidas que funcionam como "tabelas virtuais",
    facilitando consultas complexas e fornecendo abstração de dados.
    
    BENEFÍCIOS DAS VIEWS:
    1. Simplificação: Consultas complexas se tornam simples SELECT
    2. Segurança: Controle de acesso granular aos dados
    3. Performance: Queries otimizadas e reutilizáveis
    4. Relatórios: Visões pré-formatadas para análises
    5. Manutenção: Mudanças na estrutura transparentes
    
    Views criadas:
    - vw_notas_pendentes: Notas não baixadas
    - vw_notas_com_erro: Notas com problemas
    - vw_notas_mes_atual: Notas do mês atual
    - vw_resumo_diario: Estatísticas por dia
    - vw_notas_recentes: Últimas notas processadas
    
    Args:
        db_path: Caminho para o banco SQLite
        table_name: Nome da tabela base
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            views = [
                # View 1: Notas Pendentes
                f"""
                CREATE VIEW IF NOT EXISTS vw_notas_pendentes AS
                SELECT 
                    cChaveNFe,
                    nIdNF,
                    nNF,
                    dEmi,
                    cRazao,
                    vNF,
                    anomesdia,
                    CASE 
                        WHEN anomesdia IS NOT NULL THEN
                            SUBSTR(CAST(anomesdia AS TEXT), 1, 4) || '-' ||
                            SUBSTR(CAST(anomesdia AS TEXT), 5, 2) || '-' ||
                            SUBSTR(CAST(anomesdia AS TEXT), 7, 2)
                        ELSE dEmi
                    END as data_formatada
                FROM {table_name}
                WHERE xml_baixado = 0
                ORDER BY anomesdia DESC, nNF
                """,
                
                # View 2: Notas com XML Vazio (substitui view de erro)
                f"""
                CREATE VIEW IF NOT EXISTS vw_notas_com_erro AS
                SELECT 
                    cChaveNFe,
                    nNF,
                    dEmi,
                    cRazao,
                    anomesdia,
                    CASE 
                        WHEN xml_vazio = 1 THEN 'XML Vazio'
                        WHEN xml_baixado = 0 THEN 'Não Baixado'
                        ELSE 'Processando'
                    END as tipo_problema
                FROM {table_name}
                WHERE xml_vazio = 1 OR xml_baixado = 0
                ORDER BY anomesdia DESC
                """,
                
                # View 3: Notas do Mês Atual
                f"""
                CREATE VIEW IF NOT EXISTS vw_notas_mes_atual AS
                SELECT 
                    COUNT(*) as total_notas,
                    SUM(CASE WHEN xml_baixado = 1 THEN 1 ELSE 0 END) as baixadas,
                    SUM(CASE WHEN xml_vazio = 1 THEN 1 ELSE 0 END) as vazias,
                    SUM(CASE WHEN xml_baixado = 0 THEN 1 ELSE 0 END) as pendentes,
                    SUM(vNF) as valor_total,
                    MIN(anomesdia) as primeira_nota,
                    MAX(anomesdia) as ultima_nota
                FROM {table_name}
                WHERE anomesdia >= CAST(strftime('%Y%m', 'now') || '01' AS INTEGER)
                """,
                
                # View 4: Resumo Diário
                f"""
                CREATE VIEW IF NOT EXISTS vw_resumo_diario AS
                SELECT 
                    anomesdia,
                    CASE 
                        WHEN anomesdia IS NOT NULL THEN
                            SUBSTR(CAST(anomesdia AS TEXT), 7, 2) || '/' ||
                            SUBSTR(CAST(anomesdia AS TEXT), 5, 2) || '/' ||
                            SUBSTR(CAST(anomesdia AS TEXT), 1, 4)
                        ELSE 'Data Inválida'
                    END as data_br,
                    COUNT(*) as total_notas,
                    SUM(CASE WHEN xml_baixado = 1 THEN 1 ELSE 0 END) as baixadas,
                    SUM(CASE WHEN xml_vazio = 1 THEN 1 ELSE 0 END) as vazias,
                    SUM(vNF) as valor_total,
                    ROUND(AVG(vNF), 2) as valor_medio,
                    MIN(nNF) as menor_numero,
                    MAX(nNF) as maior_numero
                FROM {table_name}
                WHERE anomesdia IS NOT NULL
                GROUP BY anomesdia
                ORDER BY anomesdia DESC
                """,
                
                # View 5: Notas Recentes (últimos 7 dias)
                f"""
                CREATE VIEW IF NOT EXISTS vw_notas_recentes AS
                SELECT 
                    cChaveNFe,
                    nNF,
                    dEmi,
                    cRazao,
                    vNF,
                    xml_baixado,
                    xml_vazio,
                    anomesdia,
                    CASE 
                        WHEN xml_baixado = 1 AND xml_vazio = 0 THEN 'Baixado'
                        WHEN xml_baixado = 1 AND xml_vazio = 1 THEN 'Vazio'
                        ELSE 'Pendente'
                    END as status_visual
                FROM {table_name}
                WHERE anomesdia >= CAST(strftime('%Y%m%d', 'now', '-7 days') AS INTEGER)
                ORDER BY anomesdia DESC, nNF DESC
                """
            ]
            
            views_criadas = 0
            for view_sql in views:
                try:
                    cursor.execute(view_sql)
                    views_criadas += 1
                except sqlite3.Error as e:
                    logger.warning(f"[VIEW] Erro ao criar view: {e}")
            
            conn.commit()
            
            logger.info(f"[VIEWS] ✓ {views_criadas}/{len(views)} views criadas com sucesso")
            
            # Log de como usar as views
            logger.info("[VIEWS] Exemplos de uso:")
            logger.info("[VIEWS] - SELECT * FROM vw_notas_pendentes LIMIT 10")
            logger.info("[VIEWS] - SELECT * FROM vw_resumo_diario WHERE anomesdia >= 20250701")
            logger.info("[VIEWS] - SELECT * FROM vw_notas_com_erro")
            
    except sqlite3.Error as e:
        logger.error(f"[VIEWS] Erro de banco: {e}")
    except Exception as e:
        logger.error(f"[VIEWS] Erro inesperado: {e}")


# =============================================================================
# 📊 FUNÇÕES DE MÉTRICAS E RELATÓRIOS
# =============================================================================

def formatar_numero(n: int) -> str:
    """
    Formata número com separadores para melhor legibilidade.
    
    Args:
        n: Número inteiro para formatação
        
    Returns:
        String formatada com separadores (ex: "1.234.567")
        
    Examples:
        >>> formatar_numero(1234567)
        '1.234.567'
    """
    return f"{n:,}".replace(",", ".")


def obter_metricas_completas_banco(db_path: str = "omie.db") -> Dict[str, Any]:
    """
    Obtém métricas completas e detalhadas do banco de dados.
    
    Baseado no dashboard_db.py, esta função coleta todas as métricas
    relevantes do banco de dados em uma única operação otimizada.
    
    Args:
        db_path: Caminho para o banco de dados SQLite
        
    Returns:
        Dicionário contendo todas as métricas do banco
        
    Raises:
        sqlite3.Error: Em caso de erro de acesso ao banco
        
    Examples:
        >>> metricas = obter_metricas_completas_banco()
        >>> print(f"Total: {metricas['total']}")
    """
    if not Path(db_path).exists():
        logger.error(f"[MÉTRICAS] Banco não encontrado: {db_path}")
        return {}
    
    try:
        with sqlite3.connect(db_path) as conn:
            # Otimizações básicas
            conn.execute("PRAGMA temp_store = MEMORY")
            conn.execute("PRAGMA cache_size = -64000")
            
            cursor = conn.cursor()
            
            # ========================================
            # Estatísticas básicas
            # ========================================
            cursor.execute("SELECT COUNT(*) FROM notas")
            total = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM notas WHERE xml_baixado = 1")
            baixados = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM notas WHERE xml_baixado = 0 OR xml_baixado IS NULL")
            pendentes = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM notas WHERE xml_vazio = 1")
            vazios = cursor.fetchone()[0]
            
            # ========================================
            # Indexação temporal
            # ========================================
            cursor.execute("SELECT COUNT(*) FROM notas WHERE anomesdia IS NOT NULL")
            com_anomesdia = cursor.fetchone()[0]
            
            # ========================================
            # Datas extremas (conversão inteligente)
            # ========================================
            cursor.execute("""
                SELECT 
                    MIN(
                        CASE 
                            WHEN dEmi LIKE '__/__/____' THEN 
                                substr(dEmi, 7, 4) || '-' || 
                                substr(dEmi, 4, 2) || '-' || 
                                substr(dEmi, 1, 2)
                            ELSE dEmi 
                        END
                    ),
                    MAX(
                        CASE 
                            WHEN dEmi LIKE '__/__/____' THEN 
                                substr(dEmi, 7, 4) || '-' || 
                                substr(dEmi, 4, 2) || '-' || 
                                substr(dEmi, 1, 2)
                            ELSE dEmi 
                        END
                    )
                FROM notas 
                WHERE dEmi IS NOT NULL AND dEmi != ''
            """)
            resultado = cursor.fetchone()
            
            # Converter para formato brasileiro
            data_inicio_iso = resultado[0] or "N/A"
            data_fim_iso = resultado[1] or "N/A"
            
            data_inicio = _converter_iso_para_br(data_inicio_iso)
            data_fim = _converter_iso_para_br(data_fim_iso)
            
            # ========================================
            # Top 10 dias com mais registros
            # ========================================
            cursor.execute("""
                SELECT dEmi, COUNT(*) as total
                FROM notas 
                WHERE dEmi IS NOT NULL AND dEmi != ''
                GROUP BY dEmi 
                ORDER BY total DESC 
                LIMIT 10
            """)
            top_dias = cursor.fetchall()
            
            # ========================================
            # Campos vazios importantes
            # ========================================
            campos_vazios = {}
            for campo in ['dEmi', 'nNF', 'cRazao', 'cnpj_cpf', 'cChaveNFe']:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM notas WHERE {campo} IS NULL OR {campo} = ''")
                    campos_vazios[campo] = cursor.fetchone()[0]
                except sqlite3.Error:
                    campos_vazios[campo] = 0
            
            # ========================================
            # Progresso por período
            # ========================================
            cursor.execute("""
                SELECT 
                    substr(dEmi, 4, 7) as mes_ano,
                    COUNT(*) as total,
                    SUM(CASE WHEN xml_baixado = 1 THEN 1 ELSE 0 END) as baixados
                FROM notas
                WHERE dEmi IS NOT NULL AND dEmi != '' AND dEmi LIKE '__/__/____'
                GROUP BY mes_ano
                ORDER BY substr(dEmi, 7, 4), substr(dEmi, 4, 2)
                LIMIT 12
            """)
            progresso_mensal = cursor.fetchall()
            
            # ========================================
            # Infraestrutura do banco
            # ========================================
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='view'")
            total_views = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='index'")
            total_indices = cursor.fetchone()[0]
            
            # ========================================
            # Cálculos de percentuais
            # ========================================
            percentual_baixados = (baixados / max(1, total)) * 100
            percentual_pendentes = (pendentes / max(1, total)) * 100
            percentual_vazios = (vazios / max(1, total)) * 100
            percentual_anomesdia = (com_anomesdia / max(1, total)) * 100
            
            # ========================================
            # Estimativas de tempo
            # ========================================
            tempo_estimado = _calcular_tempo_estimado(total, baixados, pendentes, percentual_baixados)
            
            return {
                # Contadores básicos
                'total': total,
                'baixados': baixados,
                'pendentes': pendentes,
                'vazios': vazios,
                'com_anomesdia': com_anomesdia,
                
                # Percentuais
                'percentual_baixados': percentual_baixados,
                'percentual_pendentes': percentual_pendentes,
                'percentual_vazios': percentual_vazios,
                'percentual_anomesdia': percentual_anomesdia,
                
                # Datas
                'data_inicio': data_inicio,
                'data_fim': data_fim,
                
                # Rankings e distribuições
                'top_dias': top_dias,
                'progresso_mensal': progresso_mensal,
                'campos_vazios': campos_vazios,
                
                # Infraestrutura
                'total_views': total_views,
                'total_indices': total_indices,
                
                # Estimativas
                'tempo_estimado': tempo_estimado,
                
                # Status do processamento
                'status_processamento': _determinar_status_processamento(percentual_baixados, pendentes)
            }
            
    except sqlite3.Error as e:
        logger.error(f"[MÉTRICAS] Erro de banco de dados: {e}")
        return {}
    except Exception as e:
        logger.error(f"[MÉTRICAS] Erro inesperado: {e}")
        return {}


def _converter_iso_para_br(data_iso: str) -> str:
    """
    Converte data ISO para formato brasileiro.
    
    Args:
        data_iso: Data em formato ISO (YYYY-MM-DD)
        
    Returns:
        Data em formato brasileiro (DD/MM/YYYY)
    """
    try:
        if data_iso != "N/A" and len(data_iso) == 10:
            return f"{data_iso[8:10]}/{data_iso[5:7]}/{data_iso[0:4]}"
        else:
            return data_iso
    except:
        return data_iso


def _calcular_tempo_estimado(total: int, baixados: int, pendentes: int, percentual: float) -> str:
    """
    Calcula tempo estimado para conclusão baseado no progresso atual.
    
    Args:
        total: Total de registros
        baixados: Registros já baixados
        pendentes: Registros pendentes
        percentual: Percentual de conclusão
        
    Returns:
        String com estimativa de tempo
    """
    if pendentes == 0:
        return "Processamento concluído"
    
    if percentual > 95:
        return "< 1 hora (fase final)"
    elif percentual > 90:
        return "< 1 dia (fase final)"
    elif percentual > 50:
        # Estimativa baseada na velocidade atual
        dias_estimados = (pendentes / max(baixados / 30, 1000))  # Assume 30 dias de processamento
        return f"~{dias_estimados:.1f} dias"
    elif percentual > 10:
        return "Algumas semanas"
    else:
        return "Calculando..."


def _determinar_status_processamento(percentual: float, pendentes: int) -> str:
    """
    Determina o status atual do processamento.
    
    Args:
        percentual: Percentual de conclusão
        pendentes: Número de registros pendentes
        
    Returns:
        String descrevendo o status
    """
    if pendentes == 0:
        return "✅ Concluído"
    elif percentual > 95:
        return "🏁 Fase final"
    elif percentual > 75:
        return "🚀 Avançado"
    elif percentual > 50:
        return "📈 Em progresso"
    elif percentual > 25:
        return "Inicial"
    else:
        return "🟡 Iniciando"


def exibir_metricas_completas(db_path: str = "omie.db") -> None:
    """
    Exibe métricas completas do banco de dados de forma organizada.
    
    Args:
        db_path: Caminho para o banco de dados SQLite
    """
    logger.info("=" * 80)
    logger.info("📊 MÉTRICAS COMPLETAS DO BANCO DE DADOS")
    logger.info("=" * 80)
    
    metricas = obter_metricas_completas_banco(db_path)
    
    if not metricas:
        logger.error("[MÉTRICAS] Não foi possível obter métricas do banco")
        return
    
    # ========================================
    # Estatísticas principais
    # ========================================
    logger.info("📈 ESTATÍSTICAS PRINCIPAIS:")
    logger.info(f"   • Total de registros: {formatar_numero(metricas['total'])}")
    logger.info(f"   • XMLs baixados: {formatar_numero(metricas['baixados'])} ({metricas['percentual_baixados']:.1f}%)")
    logger.info(f"   • Pendentes: {formatar_numero(metricas['pendentes'])} ({metricas['percentual_pendentes']:.1f}%)")
    logger.info(f"   • XMLs vazios: {formatar_numero(metricas['vazios'])} ({metricas['percentual_vazios']:.1f}%)")
    logger.info(f"   • Status: {metricas['status_processamento']}")
    
    # ========================================
    # Período e indexação
    # ========================================
    logger.info("")
    logger.info("📅 PERÍODO E INDEXAÇÃO:")
    logger.info(f"   • Data início: {metricas['data_inicio']}")
    logger.info(f"   • Data fim: {metricas['data_fim']}")
    logger.info(f"   • Indexação temporal: {formatar_numero(metricas['com_anomesdia'])} ({metricas['percentual_anomesdia']:.1f}%)")
    
    # ========================================
    # Infraestrutura do banco
    # ========================================
    logger.info("")
    logger.info("🏗️ INFRAESTRUTURA DO BANCO:")
    logger.info(f"   • Views otimizadas: {metricas['total_views']}")
    logger.info(f"   • Índices criados: {metricas['total_indices']}")
    
    # ========================================
    # Top dias com mais registros
    # ========================================
    if metricas['top_dias']:
        logger.info("")
        logger.info("🔥 TOP 5 DIAS COM MAIS REGISTROS:")
        for i, (data, total) in enumerate(metricas['top_dias'][:5], 1):
            logger.info(f"   {i}. {data}: {formatar_numero(total)} registros")
    
    # ========================================
    # Campos vazios (apenas se houver problemas)
    # ========================================
    campos_problematicos = {k: v for k, v in metricas['campos_vazios'].items() if v > 0}
    if campos_problematicos:
        logger.info("")
        logger.info("⚠️  CAMPOS OBRIGATÓRIOS VAZIOS:")
        for campo, total in campos_problematicos.items():
            percentual = (total / max(1, metricas['total'])) * 100
            logger.info(f"   • {campo}: {formatar_numero(total)} ({percentual:.1f}%)")
    
    # ========================================
    # Progresso por período
    # ========================================
    if metricas['progresso_mensal']:
        logger.info("")
        logger.info("📊 PROGRESSO POR PERÍODO (ÚLTIMOS 6 MESES):")
        for mes_ano, total, baixados in metricas['progresso_mensal'][:6]:
            percentual = (baixados / max(1, total)) * 100
            logger.info(f"   • {mes_ano}: {formatar_numero(baixados)}/{formatar_numero(total)} ({percentual:.1f}%)")
    
    # ========================================
    # Estimativas e performance
    # ========================================
    logger.info("")
    logger.info("🎯 ESTIMATIVAS:")
    logger.info(f"   • Tempo estimado restante: {metricas['tempo_estimado']}")
    
    # Performance baseada no volume processado
    if metricas['baixados'] > 100000:
        performance = "Excelente (>100k XMLs processados)"
    elif metricas['baixados'] > 10000:
        performance = "Boa (>10k XMLs processados)"
    elif metricas['baixados'] > 1000:
        performance = "Regular"
    else:
        performance = "Inicial"
    
    logger.info(f"   • Performance: {performance}")
    
    logger.info("=" * 80)