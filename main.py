# main_pipeline.py
import logging
import configparser
import asyncio
import time
import json
import importlib
import sys
from inspect import iscoroutinefunction
from pathlib import Path
from datetime import datetime

from src import (
    report_arquivos_vazios,
    verificador_xmls,
    atualizar_query_params_ini,
    extrator_async,
    compactador_resultado,
    atualizar_caminhos_arquivos
)
from src.utils import registrar_metricas_banco
from src.upload_onedrive import OneDriveUploader

CONFIG_PATH = "configuracao.ini"
METRICAS_JSON = "log/metricas_execucao.json"


def configurar_logging() -> None:
    Path("log").mkdir(exist_ok=True)
    log_file = Path("log") / f"main_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )


def carregar_configuracoes(config_path: str = CONFIG_PATH) -> dict:
    if not Path(config_path).exists():
        logging.error(f"[CONFIG] Arquivo INI não encontrado: {config_path}")
        sys.exit(1)

    config = configparser.ConfigParser()
    config.read(config_path)

    try:
        return {
            "resultado_dir": config["paths"]["resultado_dir"],
            "modo_download": config.get("api_speed", "modo_download", fallback="async").lower(),
            "db_name": config["paths"].get("db_name", "omie.db"),
            "upload_onedrive": config.getboolean("onedrive", "upload_onedrive", fallback=False)
        }
    except Exception as e:
        logging.exception(f"[CONFIG] Erro ao carregar configurações do INI: {e}")
        sys.exit(1)


def benchmark(func):
    def wrapper(*args, **kwargs):
        inicio = time.perf_counter()
        resultado = func(*args, **kwargs)
        duracao = time.perf_counter() - inicio
        logging.info(f"[ TEMPO] {func.__name__} finalizada em {duracao:.2f} segundos")
        return resultado, duracao
    return wrapper


@benchmark
def executar_pipeline(config: dict):
    if config["modo_download"] == "async":
        logging.info("[PIPELINE] Executando modo assíncrono...")
        if iscoroutinefunction(extrator_async.main):
            asyncio.run(extrator_async.main())
        else:
            raise RuntimeError("extrator_async.main() não é uma coroutine.")
    else:
        logging.info("[PIPELINE] Executando modo paralelo...")
        baixar_parallel = importlib.import_module("baixar_parallel_v2")
        baixar_parallel.main()


@benchmark
def executar_verificador_xmls():
    verificador_xmls.verificar()


@benchmark
def executar_atualizacao_de_caminhos():
    atualizar_caminhos_arquivos.atualizar_caminhos_no_banco()


@benchmark
def executar_compactador_resultado(resultado_dir: str):
    compactador_resultado.zipar_pastas_sem_zip(resultado_dir)


@benchmark
def executar_relatorio_arquivos_vazios(resultado_dir: str):
    report_arquivos_vazios.gerar_relatorio(resultado_dir)


@benchmark
def executar_upload(resultado_dir: str, config: dict):
    if not config.get("upload_onedrive", False):
        logging.info("[UPLOAD] Upload para OneDrive desativado no INI.")
        return
    uploader = OneDriveUploader()
    arquivos = list(Path(resultado_dir).rglob("*_compactado.zip"))
    if not arquivos:
        logging.warning("[UPLOAD] Nenhum arquivo .zip encontrado para upload.")
        return
    for arquivo in arquivos:
        uploader.upload_file(arquivo)


def gerar_resumo_final(metricas: dict, duracao_total: float) -> None:
    try:
        logging.info("[RESUMO] Métricas consolidadas da execução:")
        for etapa, tempo in metricas.items():
            logging.info(f" - {etapa}: {tempo:.2f}s")
        registrar_metricas_banco("omie.db")
        logging.info(f"[RESUMO] Duração total: {duracao_total:.2f}s")

        with open(METRICAS_JSON, "w") as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "metricas": metricas,
                "duracao_total": duracao_total
            }, f, indent=4)
    except Exception as e:
        logging.exception(f"[RESUMO] Falha ao gerar resumo: {e}")


def main():
    configurar_logging()
    logging.info("Iniciando pipeline do Extrator Omie V3...")
    config = carregar_configuracoes()
    resultado_dir = config["resultado_dir"]

    metricas: dict[str, float] = {}
    inicio_total = time.perf_counter()

    #executar_atualizador_datas_query()  

    _, tempo = executar_pipeline(config)
    metricas["extracao_pipeline"] = tempo

    _, tempo = executar_verificador_xmls()
    metricas["verificacao_xmls"] = tempo

    _, tempo = executar_atualizacao_de_caminhos()
    metricas["atualizacao_caminhos"] = tempo

    _, tempo = executar_compactador_resultado(resultado_dir)
    metricas["compactacao_resultados"] = tempo

    _, tempo = executar_relatorio_arquivos_vazios(resultado_dir)
    metricas["relatorio_xmls_vazios"] = tempo

    _, tempo = executar_upload(resultado_dir, config)
    metricas["upload_onedrive"] = tempo

    duracao_total = time.perf_counter() - inicio_total
    gerar_resumo_final(metricas, duracao_total)

    logging.info(" Pipeline finalizada com sucesso.")


if __name__ == "__main__":
    main()
