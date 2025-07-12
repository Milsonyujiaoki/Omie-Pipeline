# baixar_parallel_v8.py

import os
import html
import sqlite3
import logging
import requests
import configparser
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

# Importa funções utilitárias do projeto
from utils import atualizar_status_xml, iniciar_db, salvar_nota

# === Leitura de configurações do arquivo INI ===
config = configparser.ConfigParser()
config.read('configuracao.ini')

# Parâmetros da API
APP_KEY = config['omie_api']['app_key']
APP_SECRET = config['omie_api']['app_secret']
START_DATE = config['query_params']['start_date']
END_DATE = config['query_params']['end_date']
RECORDS_PER_PAGE = int(config['query_params']['records_per_page'])

# Parâmetros do ambiente
DB_NAME = config['paths'].get('db_name', 'omie.db')  # Nome do banco local
TABLE_NAME = 'notas'
TIMEOUT = int(config['api_speed'].get('timeout', 60))  # Timeout em segundos por requisição
MAX_WORKERS = int(config['api_speed'].get('parallel_workers', 4))  # Nº máximo de workers paralelos

# URLs das APIs Omie
URL_LISTAR = config['omie_api'].get('base_url_nf', 'https://app.omie.com.br/api/v1/produtos/nfconsultar/')
URL_XML = config['omie_api'].get('base_url_xml', 'https://app.omie.com.br/api/v1/produtos/dfedocs/')


def listar_nfs() -> None:
    """
    Consulta a API Omie para listar notas fiscais emitidas no período especificado
    e insere os dados relevantes no banco de dados local SQLite.
    """
    pagina = 1
    while True:
        payload = {
            'app_key': APP_KEY,
            'app_secret': APP_SECRET,
            'call': 'ListarNF',
            'param': [{
                'pagina': pagina,
                'registros_por_pagina': RECORDS_PER_PAGE,
                'apenas_importado_api': 'N',
                'dEmiInicial': START_DATE,
                'dEmiFinal': END_DATE,
                'tpNF': 1,
                'tpAmb': 1,
                'cDetalhesPedido': 'N',
                'cApenasResumo': 'S',
                'ordenar_por': 'CODIGO'
            }]
        }
        try:
            # Chamada HTTP para listar as NFs
            response = requests.post(URL_LISTAR, json=payload, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()
            notas = data.get('nfCadastro', [])

            # Salva cada nota encontrada no banco
            for nf in notas:
                salvar_nota({
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
                    'vNF': nf['total']['ICMSTot'].get('vNF')
                }, DB_NAME)

            # Controle de paginação
            total_paginas = data.get('total_de_paginas', 1)
            logging.info(f"Página {pagina}/{total_paginas} importada.")
            if pagina >= total_paginas:
                break
            pagina += 1

        except Exception as e:
            logging.error(f"Erro ao listar página {pagina}: {e}")
            break


def baixar_uma_nota(registro: tuple) -> Optional[str]:
    """
    Faz o download de um único XML com base nos dados do banco.

    Args:
        registro: Tupla (nIdNF, cChaveNFe, dEmi, nNF).

    Returns:
        A chave da nota baixada, ou None em caso de falha.
    """
    nIdNF, chave, data_emissao, num_nfe = registro
    try:
        # Define nome e caminho de salvamento do XML
        data_dt = datetime.strptime(data_emissao, '%d/%m/%Y')
        nome_arquivo = f"{num_nfe}_{data_dt.strftime('%Y%m%d')}_{chave}.xml"
        pasta = Path("resultado") / data_dt.strftime('%Y') / data_dt.strftime('%m') / data_dt.strftime('%d')
        caminho = pasta / nome_arquivo

        pasta.mkdir(parents=True, exist_ok=True)
        rebaixado = caminho.exists()  # Verifica se já existia antes

        # Prepara a requisição para obter o XML
        payload = {
            'call': 'ObterNfe',
            'app_key': APP_KEY,
            'app_secret': APP_SECRET,
            'param': [{'nIdNfe': nIdNF}]
        }

        response = requests.post(URL_XML, headers={'Content-Type': 'application/json'}, json=payload, timeout=TIMEOUT)
        response.raise_for_status()
        data = response.json()

        # Salva o conteúdo XML no disco
        xml_str = html.unescape(data['cXmlNfe'])
        caminho.write_text(xml_str, encoding='utf-8')

        # Atualiza status no banco
        atualizar_status_xml(DB_NAME, chave, caminho, xml_str, rebaixado)
        logging.info(f"XML salvo: {caminho}")
        return chave

    except Exception as e:
        logging.warning(f"Erro ao baixar nota {chave}: {e}")
        return None


def baixar_xmls_em_parallel() -> None:
    """
    Realiza o download dos XMLs em paralelo usando ThreadPoolExecutor,
    buscando do banco de dados apenas os registros ainda não baixados.
    """
    with sqlite3.connect(DB_NAME) as conn:
        rows = conn.execute(
            f"SELECT nIdNF, cChaveNFe, dEmi, nNF FROM {TABLE_NAME} WHERE xml_baixado = 0"
        ).fetchall()

    logging.info(f" Iniciando download paralelo de {len(rows)} XMLs com {MAX_WORKERS} workers...")

    # Execução paralela utilizando pool de threads
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(baixar_uma_nota, row): row for row in rows}
        for future in as_completed(futures):
            future.result()  # Executa e trata eventual erro dentro da função


def main():
    """
    Função principal que executa toda a pipeline:
    1. Inicializa log e banco.
    2. Lista notas via API.
    3. Baixa XMLs em paralelo.
    """
    # Garante existência do diretório de logs
    Path("log").mkdir(exist_ok=True)

    # Configura logging com nome do log baseado em data/hora
    logging.basicConfig(
        filename=f"log/baixar_parallel_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
        level=logging.INFO,
        format='%(asctime)s - %(message)s'
    )

    logging.info(" Iniciando execução completa do modo paralelo: Listagem + Download")
    
    iniciar_db(DB_NAME, TABLE_NAME)  # Cria a tabela se necessário
    listar_nfs()                     # Lista e salva NFs
    baixar_xmls_em_parallel()       # Faz download em paralelo
    logging.info("Execução finalizada com sucesso.")


# Entry point do script
if __name__ == '__main__':
    main()
