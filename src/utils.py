import sqlite3
from pathlib import Path
import logging
from datetime import datetime
import re
from typing import Optional

def normalizar_data(data: Optional[str]) -> Optional[str]:
    try:
        return datetime.strptime(data, '%d/%m/%Y').strftime('%Y-%m-%d') if data else None
    except Exception as e:
        logging.warning(f"[DATA] Erro ao normalizar data '{data}': {e}")
        return None

def sanitizar_cnpj(valor: str) -> str:
    return re.sub(r'\D', '', str(valor or ''))

def normalizar_valor_nf(valor: Optional[str | float]) -> float:
    try:
        return float(valor) if valor else 0.0
    except Exception as e:
        logging.warning(f"[ALERT] Valor vNF inválido: {valor} - {e}")
        return 0.0

def transformar_em_tuple(registro: dict) -> tuple:
    return (
        registro['cChaveNFe'],
        registro.get('nIdNF'),
        registro.get('nIdPedido'),
        registro.get('dCan'),
        normalizar_data(registro.get('dEmi')),
        registro.get('dInut'),
        normalizar_data(registro.get('dReg')),
        normalizar_data(registro.get('dSaiEnt')),
        registro.get('hEmi'),
        registro.get('hSaiEnt'),
        registro.get('mod'),
        registro.get('nNF'),
        registro.get('serie'),
        registro.get('tpAmb'),
        registro.get('tpNF'),
        sanitizar_cnpj(registro.get('cnpj_cpf')),
        registro.get('cRazao'),
        normalizar_valor_nf(registro.get('vNF')),
        None,  # caminho_arquivo
        0,     # baixado_novamente
        0      # xml_vazio
    )

def gerar_xml_path(
    chave: str,
    data_emissao: str,
    num_nfe: str,
    base_dir: str = "resultado"
) -> tuple[Path, Path]:
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
    if not chave:
        logging.warning("[ERRO] Chave não fornecida para atualização do XML.")
        return

    caminho_arquivo = str(caminho.resolve())
    xml_vazio = 1 if xml_str.strip() == '' else 0
    baixado_novamente = 1 if rebaixado else 0

    if not caminho.exists():
        logging.warning(f"[ERRO] Caminho {caminho_arquivo} não existe no disco.")
        return

    if not caminho_arquivo:
        logging.warning(f"[ERRO] Caminho do XML está vazio para chave {chave}.")
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
                SET xml_baixado = 1, caminho_arquivo = ?, baixado_novamente = ?, xml_vazio = ? 
                WHERE cChaveNFe = ? AND (? IS NOT NULL)
                """,
                (caminho_arquivo, baixado_novamente, xml_vazio, chave, caminho_arquivo)
            )
            if cursor.rowcount == 0:
                logging.warning(f"[ALERT] Nenhum registro atualizado para chave: {chave}")
            conn.commit()
    except Exception as e:
        logging.exception(f"[ERRO] Falha ao atualizar status do XML para {chave}: {e}")

def iniciar_db(db_path: str, table_name: str = "notas") -> None:
    logging.info("[DB] Inicializando banco e tabela...")
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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_chave ON notas (cChaveNFe)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_baixado ON notas (xml_baixado)")
        conn.commit()
    logging.info("[DB] Banco e índices inicializados com sucesso.")

def salvar_nota(registro: dict, db_path: str) -> None:
    chave = registro.get('cChaveNFe')
    if not chave:
        logging.warning("[ALERT] Registro sem chave fiscal: ignorado.")
        return

    try:
        valores = transformar_em_tuple(registro)
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
                valores
            )
            conn.commit()
            logging.debug(f"[DB] Nota {chave} inserida com sucesso.")
    except sqlite3.IntegrityError:
        logging.debug(f"[DUPLICADO] Nota já existente: {chave}")
    except Exception as e:
        logging.exception(f"[ERRO] Falha ao inserir nota {chave}: {e}")

def salvar_varias_notas(registros: list[dict], db_path: str) -> None:
    logging.info(f"[DB] Inserindo {len(registros)} notas em lote...")
    dados = []

    for r in registros:
        chave = r.get('cChaveNFe')
        if not chave:
            logging.warning("[ALERT] Registro sem chave ignorado.")
            continue
        try:
            dados.append(transformar_em_tuple(r))
        except Exception as e:
            logging.warning(f"[ALERT] Erro ao preparar nota {chave}: {e}")

    if not dados:
        logging.info("[DB] Nenhuma nota válida para inserir.")
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
            logging.info(f"[DB] {len(dados)} notas inseridas com sucesso.")
    except Exception as e:
        logging.exception(f"[ERRO] Falha ao inserir notas em lote: {e}")
