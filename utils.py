# utils.py
# Requer: Python 3.9+
# Dependências: sqlite3, pathlib, logging, datetime

import sqlite3
from pathlib import Path
import logging
from datetime import datetime

def gerar_xml_path(
    chave: str,
    data_emissao: str,
    num_nfe: str,
    base_dir: str = "resultado"
) -> tuple[Path, Path]:
    """
    Gera o caminho completo para salvar o arquivo XML da nota fiscal com base
    na chave, data de emissão e número da NFe.

    Args:
        chave: Chave da nota fiscal eletrônica.
        data_emissao: Data de emissão no formato dd/mm/aaaa.
        num_nfe: Número da nota fiscal.
        base_dir: Diretório base onde os XMLs serão salvos (padrão: 'resultado').

    Returns:
        Tupla com:
            - pasta onde o XML será salvo (Path)
            - caminho completo do arquivo XML (Path)
    """
    try:
        data_dt = datetime.strptime(data_emissao, '%d/%m/%Y')
    except ValueError as e:
        raise ValueError(f"Data de emissão inválida '{data_emissao}': {e}")

    nome_arquivo = f"{num_nfe}_{data_dt.strftime('%Y%m%d')}_{chave}.xml"
    pasta = Path(base_dir) / data_dt.strftime('%Y') / data_dt.strftime('%m') / data_dt.strftime('%d')
    caminho = pasta / nome_arquivo
    return pasta, caminho

def atualizar_status_xml(
    db_path: str,
    chave: str,
    caminho: Path,
    xml_str: str,
    rebaixado: bool = False
) -> None:
    """
    Atualiza os campos relacionados ao status do XML no banco de dados SQLite.

    Args:
        db_path: Caminho para o banco de dados SQLite.
        chave: Chave da nota fiscal (cChaveNFe).
        caminho: Caminho completo onde o XML foi salvo.
        xml_str: Conteúdo do XML como string.
        rebaixado: Define se o XML já existia antes (evita sobregravação não desejada).
    """
    if not chave:
        logging.warning("[ERRO] Chave não fornecida para atualização do XML.")
        return

    caminho_arquivo = str(caminho.resolve())
    xml_vazio = 1 if xml_str.strip() == '' else 0
    baixado_novamente = 1 if rebaixado else 0

    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")

            conn.execute("CREATE INDEX IF NOT EXISTS idx_chave ON notas (cChaveNFe)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_baixado ON notas (xml_baixado)")

            cursor = conn.cursor()
            cursor.execute(
                f"UPDATE notas SET xml_baixado = 1, caminho_arquivo = ?, baixado_novamente = ?, xml_vazio = ? WHERE cChaveNFe = ?",
                (caminho_arquivo, baixado_novamente, xml_vazio, chave)
            )
            if cursor.rowcount == 0:
                logging.warning(f"[⚠️] Nenhum registro encontrado para chave: {chave}")
            conn.commit()
    except Exception as e:
        logging.exception(f"[ERRO] Falha ao atualizar status do XML para {chave}: {e}")

def iniciar_db(db_path: str, table_name: str = "notas") -> None:
    """
    Inicializa o banco de dados SQLite com a tabela 'notas', caso ela ainda não exista.

    Args:
        db_path: Caminho para o banco de dados.
        table_name: Nome da tabela a ser criada/verificada (padrão: 'notas').
    """
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")

        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                cChaveNFe TEXT PRIMARY KEY,
                nIdNF INTEGER,
                nIdPedido INTEGER,
                dCan TEXT,
                dEmi TEXT,
                dInut TEXT,
                dReg TEXT,
                dSaiEnt TEXT,
                hEmi TEXT,
                hSaiEnt TEXT,
                mod TEXT,
                nNF TEXT,
                serie TEXT,
                tpAmb TEXT,
                tpNF TEXT,
                cnpj_cpf TEXT,
                cRazao TEXT,
                vNF REAL,
                xml_baixado BOOLEAN DEFAULT 0,
                caminho_arquivo TEXT DEFAULT NULL,
                baixado_novamente BOOLEAN DEFAULT 0,
                xml_vazio BOOLEAN DEFAULT 0
            )
        ''')
        conn.commit()

def salvar_nota(registro: dict, db_path: str) -> None:
    """
    Insere uma nota fiscal no banco de dados, ignorando duplicatas.

    Args:
        registro: Dicionário com os dados da nota fiscal.
        db_path: Caminho para o banco de dados SQLite.
    """
    chave = registro.get('cChaveNFe')
    if not chave:
        logging.warning("[⚠️] Registro sem chave fiscal: ignorado.")
        return

    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")

            conn.execute(f'''
                INSERT INTO notas (
                    cChaveNFe, nIdNF, nIdPedido, dCan, dEmi, dInut, dReg, dSaiEnt, hEmi, hSaiEnt,
                    mod, nNF, serie, tpAmb, tpNF, cnpj_cpf, cRazao, vNF,
                    caminho_arquivo, baixado_novamente, xml_vazio
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (
                    chave,
                    registro.get('nIdNF'),
                    registro.get('nIdPedido'),
                    registro.get('dCan'),
                    registro.get('dEmi'),
                    registro.get('dInut'),
                    registro.get('dReg'),
                    registro.get('dSaiEnt'),
                    registro.get('hEmi'),
                    registro.get('hSaiEnt'),
                    registro.get('mod'),
                    registro.get('nNF'),
                    registro.get('serie'),
                    registro.get('tpAmb'),
                    registro.get('tpNF'),
                    registro.get('cnpj_cpf'),
                    registro.get('cRazao'),
                    registro.get('vNF'),
                    None,
                    0,
                    0
                )
            )
            conn.commit()
    except sqlite3.IntegrityError:
        logging.debug(f"[duplicado] Nota já existente: {chave}")

def salvar_varias_notas(registros: list[dict], db_path: str) -> None:
    """
    Insere várias notas fiscais no banco de dados em lote, ignorando duplicatas.

    Args:
        registros: Lista de dicionários com os dados das notas fiscais.
        db_path: Caminho para o banco de dados SQLite.
    """
    dados = []
    for r in registros:
        chave = r.get('cChaveNFe')
        if not chave:
            logging.warning("[ALERT] Registro sem chave ignorado em lote.")
            continue
        dados.append((
            chave,
            r.get('nIdNF'),
            r.get('nIdPedido'),
            r.get('dCan'),
            r.get('dEmi'),
            r.get('dInut'),
            r.get('dReg'),
            r.get('dSaiEnt'),
            r.get('hEmi'),
            r.get('hSaiEnt'),
            r.get('mod'),
            r.get('nNF'),
            r.get('serie'),
            r.get('tpAmb'),
            r.get('tpNF'),
            r.get('cnpj_cpf'),
            r.get('cRazao'),
            r.get('vNF'),
            None,
            0,
            0
        ))

    if not dados:
        logging.info("Nenhuma nota válida para inserir em lote.")
        return

    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")

            conn.executemany(f'''
                INSERT OR IGNORE INTO notas (
                    cChaveNFe, nIdNF, nIdPedido, dCan, dEmi, dInut, dReg, dSaiEnt, hEmi, hSaiEnt,
                    mod, nNF, serie, tpAmb, tpNF, cnpj_cpf, cRazao, vNF,
                    caminho_arquivo, baixado_novamente, xml_vazio
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', dados)
            conn.commit()
            logging.info(f"{len(dados)} notas inseridas em lote com sucesso.")
    except Exception as e:
        logging.exception(f"[ERRO] Falha ao inserir notas em lote: {e}")
