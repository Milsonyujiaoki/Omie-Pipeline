# main_pipeline_v2.py

import logging
import configparser
import asyncio
from datetime import datetime
from pathlib import Path
import importlib
import sys
from inspect import iscoroutinefunction

# Importação de módulos locais utilizados na pipeline
from src import report_arquivos_vazios
from src import verificador_xmls
from src import atualizar_query_params_ini
from src import extrator_async
from src import compactador_resultado
from src import atualizar_caminhos_arquivos

CONFIG_PATH = "configuracao.ini"  # Caminho padrão do arquivo INI


def configurar_logging() -> None:
    """
    Configura o logging da aplicação, com saída simultânea para arquivo e console.

    Cria um diretório 'log' se necessário, e define o nome do arquivo com timestamp.
    """
    log_dir = Path("log")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"main_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )


def carregar_configuracoes(config_path: str = CONFIG_PATH) -> dict:
    """
    Carrega e valida as configurações do arquivo INI.

    Args:
        config_path: Caminho do arquivo de configuração INI.

    Returns:
        Dicionário com as configurações essenciais da pipeline.
    """
    if not Path(config_path).exists():
        logging.error(f"Arquivo de configuração INI não encontrado: {config_path}")
        sys.exit(1)

    config = configparser.ConfigParser()
    config.read(config_path)

    if 'paths' not in config or 'resultado_dir' not in config['paths']:
        logging.error("Seção [paths] ou chave resultado_dir ausente no INI.")
        sys.exit(1)

    resultado_dir = config.get("paths", "resultado_dir")
    modo_download = config.get("api_speed", "modo_download", fallback="async").lower()

    return {
        "resultado_dir": resultado_dir,
        "modo_download": modo_download
    }


def executar_compactador_resultado(resultado_dir: str) -> None:
    """
    Compacta os arquivos XML em pastas do diretório de resultados.

    Args:
        resultado_dir: Diretório onde os XMLs estão salvos.
    """
    try:
        logging.info("Iniciando compactação dos resultados...")
        compactador_resultado.zipar_pastas_sem_zip(resultado_dir)
        logging.info("Compactação finalizada.")
    except Exception as e:
        logging.exception(f"Erro na compactação: {e}")


def executar_relatorio_arquivos_vazios(pasta: str) -> None:
    """
    Gera um relatório Excel com arquivos XML vazios ou de 0 bytes.

    Args:
        pasta: Caminho base onde o relatório deve varrer os arquivos.
    """
    try:
        logging.info(f"Gerando relatório de arquivos vazios em: {pasta}")
        report_arquivos_vazios.gerar_relatorio(pasta)
        logging.info("Relatório gerado.")
    except Exception as e:
        logging.exception(f"Erro no relatório de arquivos vazios: {e}")


def executar_verificador_xmls() -> None:
    """
    Verifica se arquivos XML já existem no disco e atualiza o banco de dados.
    """
    try:
        logging.info("Verificando arquivos XML existentes...")
        verificador_xmls.verificar()
        logging.info("Verificação finalizada.")
    except Exception as e:
        logging.exception(f"Erro na verificação de XMLs: {e}")


def executar_atualizador_datas_query() -> None:
    """
    Atualiza os parâmetros de data no arquivo de configuração INI.
    """
    try:
        logging.info("Atualizando datas no INI...")
        atualizar_query_params_ini.atualizar_datas_configuracao_ini()
        logging.info("Datas atualizadas.")
    except Exception as e:
        logging.exception(f"Erro ao atualizar datas no INI: {e}")


def executar_pipeline(config: dict) -> None:
    """
    Executa a extração principal dos dados, conforme o modo de download (async/paralelo).

    Args:
        config: Dicionário de configuração carregado do INI.
    """
    try:
        if config['modo_download'] == 'async':
            logging.info("Executando modo assíncrono via extrator_async...")
            if iscoroutinefunction(extrator_async.main):
                asyncio.run(extrator_async.main())
            else:
                raise RuntimeError("extrator_async.main() não é uma coroutine.")
        else:
            logging.info("Executando modo paralelo via baixar_parallel_v2...")
            baixar_parallel = importlib.import_module("baixar_parallel_v2")
            baixar_parallel.main()
    except Exception as e:
        logging.exception(f"Erro ao executar pipeline de download: {e}")


def executar_atualizacao_de_caminhos() -> None:
    """
    Atualiza os caminhos dos arquivos XML no banco de dados com base nos arquivos físicos.
    """
    try:
        logging.info("Atualizando caminhos de XMLs no banco...")
        atualizar_caminhos_arquivos.atualizar_caminhos_no_banco()
    except Exception as e:
        logging.exception(f"Erro ao atualizar caminhos de arquivos: {e}")


def main():
    """
    Função principal que orquestra a execução completa da pipeline de integração com a API Omie.
    Envolve atualização de datas, extração de dados, verificação de arquivos e geração de relatórios.
    """
    configurar_logging()
    logging.info("Iniciando pipeline do Extrator Omie V3...")

    print(f"Executando como: {sys.executable}")
    print(f"Argumentos recebidos: {sys.argv}")

    config = carregar_configuracoes()
    resultado_dir = config['resultado_dir']

    #executar_atualizador_datas_query()
    executar_pipeline(config)
    executar_verificador_xmls()
    executar_atualizacao_de_caminhos()
    executar_compactador_resultado(resultado_dir)
    executar_relatorio_arquivos_vazios(resultado_dir)

    logging.info("Pipeline completa com sucesso.")


# Ponto de entrada do script
if __name__ == "__main__":
    main()
