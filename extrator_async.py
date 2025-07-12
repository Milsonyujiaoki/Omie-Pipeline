# extrator_async_v7.py

# Importações padrão
import os
import html
import sqlite3
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from typing import Any

# Importações de terceiros
import aiohttp

# Importações internas do projeto
from .utils import atualizar_status_xml, iniciar_db, salvar_nota, gerar_xml_path
from .omie_client_async import OmieClient, carregar_configuracoes
from .omie_client_async import with_retries  # Decorador para retry automático

# Nome da tabela usada no banco SQLite
TABLE_NAME = 'notas'


async def listar_nfs(client: OmieClient, config: dict[str, Any], db_name: str):
    """
    Lista as notas fiscais (NFs) disponíveis via API Omie, página por página,
    e armazena as informações essenciais no banco SQLite.

    Args:
        client: Cliente Omie já autenticado.
        config: Configurações carregadas do arquivo de configuração (INI).
        db_name: Nome do banco de dados SQLite.
    """
    pagina = 1  # Página inicial da listagem
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as session:
        while True:
            # Monta o payload da requisição
            payload = {
                'pagina': pagina,
                'registros_por_pagina': config["records_per_page"],
                'apenas_importado_api': 'N',
                'dEmiInicial': config["start_date"],
                'dEmiFinal': config["end_date"],
                'tpNF': 1,  # Tipo da nota: 1 = Saída
                'tpAmb': 1,  # Ambiente: 1 = Produção
                'cDetalhesPedido': 'N',
                'cApenasResumo': 'S',
                'ordenar_por': 'CODIGO',
            }
            try:
                # Chama a API para listar notas fiscais
                data = await client.call_api(session, "ListarNF", payload)
                notas = data.get('nfCadastro', [])

                # Caso não existam mais notas na página atual
                if not notas:
                    logging.warning(f"Página {pagina} sem notas: {data}")
                    break

                # Para cada nota retornada, salva os dados no banco
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
                    }, db_name)

                # Verifica se chegou na última página
                total_paginas = data.get("total_de_paginas", 1)
                logging.info(f"Página {pagina} processada. Total: {total_paginas}")
                if pagina >= total_paginas:
                    break

                pagina += 1  # Avança para a próxima página

            except Exception as e:
                logging.exception(f"Erro ao listar página {pagina}: {e}")
                break


@with_retries(max_retries=3, delay=2)
async def baixar_xml_individual(
    session: aiohttp.ClientSession,
    client: OmieClient,
    row: tuple,
    semaphore: asyncio.Semaphore,
    db_name: str
):
    """
    Faz o download individual do XML da NF, respeitando o limite de concorrência (semaphore).

    Args:
        session: Sessão HTTP assíncrona.
        client: Cliente Omie para chamadas de API.
        row: Tupla com os dados da nota (nIdNF, chave, data_emissao, num_nfe).
        semaphore: Controle de concorrência para limitar chamadas simultâneas.
        db_name: Nome do banco de dados.
    """
    async with semaphore:
        nIdNF, chave, data_emissao, num_nfe = row
        try:
            # Define o caminho para salvar o XML
            pasta, caminho = gerar_xml_path(chave, data_emissao, num_nfe)
            pasta.mkdir(parents=True, exist_ok=True)
            rebaixado = caminho.exists()  # Verifica se o arquivo já existia (rebaixado)

            # Faz a chamada à API para obter o XML
            data = await client.call_api(session, "ObterNfe", {"nIdNfe": nIdNF})
            xml_str = html.unescape(data.get("cXmlNfe", ""))

            # Salva o XML no disco
            caminho.write_text(xml_str, encoding='utf-8')

            # Atualiza o status no banco de dados
            atualizar_status_xml(db_name, chave, caminho, xml_str, rebaixado)

            logging.info(f" XML salvo: {caminho}")
        except Exception as e:
            logging.error(f"Erro ao baixar {chave}: {e}")


async def baixar_xmls(client: OmieClient, db_name: str):
    """
    Realiza o download dos XMLs de todas as notas pendentes (xml_baixado = 0)
    usando concorrência assíncrona com controle via semáforo.

    Args:
        client: Cliente Omie.
        db_name: Nome do banco de dados.
    """
    # Carrega todas as notas ainda não baixadas
    with sqlite3.connect(db_name) as conn:
        rows = conn.execute(
            f"SELECT nIdNF, cChaveNFe, dEmi, nNF FROM {TABLE_NAME} WHERE xml_baixado = 0"
        ).fetchall()

    # Cria um semáforo com o mesmo limite de chamadas por segundo do cliente
    semaphore = asyncio.Semaphore(client.semaphore._value)

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        # Executa o download dos XMLs em paralelo com controle de concorrência
        await asyncio.gather(*[
            baixar_xml_individual(session, client, row, semaphore, db_name)
            for row in rows
        ])


async def main():
    """
    Função principal para orquestrar:
    1. Carregar configurações.
    2. Inicializar banco de dados.
    3. Listar notas via API.
    4. Baixar XMLs das notas.
    """
    config = carregar_configuracoes()
    db_name = config.get("db_name", "omie.db")

    # Instancia o cliente Omie com base nas configurações
    client = OmieClient(
        app_key=config["app_key"],
        app_secret=config["app_secret"],
        calls_per_second=config["calls_per_second"],
        base_url_nf=config["base_url_nf"],
        base_url_xml=config["base_url_xml"]
    )

    # Inicializa o banco de dados e a tabela
    iniciar_db(db_name, TABLE_NAME)

    # Etapa 1: Listagem de NFs
    await listar_nfs(client, config, db_name)

    # Etapa 2: Download de XMLs
    await baixar_xmls(client, db_name)


# Entry point do script
if __name__ == "__main__":
    asyncio.run(main())
