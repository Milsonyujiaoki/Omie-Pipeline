import asyncio
import aiohttp
import configparser
import logging
from typing import Any

class OmieClient:
    """
    Cliente assíncrono para a API Omie, com controle de concorrência e retry interno.
    """

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        calls_per_second: int = 4,
        base_url_nf: str = "https://app.omie.com.br/api/v1/produtos/nfconsultar/",
        base_url_xml: str = "https://app.omie.com.br/api/v1/produtos/dfedocs/"
    ):
        self.app_key = app_key
        self.app_secret = app_secret
        self.base_url_nf = base_url_nf
        self.base_url_xml = base_url_xml
        self.semaphore = asyncio.Semaphore(calls_per_second)

    async def call_api(
        self,
        session: aiohttp.ClientSession,
        metodo: str,
        params: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Realiza chamada assíncrona para a API Omie, com retry inteligente para erros 429/500.

        Args:
            session: Sessão aiohttp compartilhada.
            metodo: Nome da operação da API.
            params: Parâmetros da chamada.
        """
        url = self.base_url_nf if metodo == "ListarNF" else self.base_url_xml
        payload = {
            "app_key": self.app_key,
            "app_secret": self.app_secret,
            "call": metodo,
            "param": [params],
        }

        max_retries = 5
        for attempt in range(1, max_retries + 1):
            try:
                async with self.semaphore:
                    async with session.post(url, json=payload, timeout=60) as response:
                        status = response.status

                        if status == 429:
                            delay = 2 ** attempt
                            logging.warning(f"[API] Status 429 (rate limit). Tentativa {attempt}/{max_retries}. Retentando em {delay}s...")
                            await asyncio.sleep(delay)
                            continue

                        if 500 <= status < 600:
                            delay = 1.5 * attempt
                            logging.warning(f"[API] Erro {status} da Omie. Tentativa {attempt}/{max_retries}. Esperando {delay}s...")
                            await asyncio.sleep(delay)
                            continue

                        response.raise_for_status()  # Lança exceção para 4xx
                        resultado = await response.json()

                        if not isinstance(resultado, dict):
                            raise ValueError("[API] Resposta inesperada (não é dict)")

                        return resultado

            except aiohttp.ClientError as e:
                logging.error(f"[API] Erro de rede (tentativa {attempt}): {e}")
                await asyncio.sleep(1.5 * attempt)

            except asyncio.TimeoutError:
                logging.warning(f"[API] Timeout na tentativa {attempt}. Repetindo após espera...")
                await asyncio.sleep(1.5 * attempt)

            except Exception as e:
                logging.exception(f"[API] Falha inesperada ao chamar '{metodo}' (tentativa {attempt}): {e}")
                await asyncio.sleep(1.5 * attempt)

        raise RuntimeError(f"[API] Falha permanente ao chamar '{metodo}' após {max_retries} tentativas.")


def carregar_configuracoes(path_arquivo: str = 'configuracao.ini') -> dict[str, Any]:
    """
    Carrega e interpreta o arquivo INI com as credenciais e parâmetros da API.

    Args:
        path_arquivo: Caminho do arquivo de configuração INI.

    Returns:
        Dicionário contendo chaves de configuração da API.
    """
    config = configparser.ConfigParser()
    config.read(path_arquivo)

    return {
        "app_key": config['omie_api']['app_key'],
        "app_secret": config['omie_api']['app_secret'],
        "start_date": config['query_params']['start_date'],
        "end_date": config['query_params']['end_date'],
        "records_per_page": int(config['query_params']['records_per_page']),
        "calls_per_second": int(config['api_speed']['calls_per_second']),
        "base_url_nf": config.get(
            'omie_api',
            'base_url_nf',
            fallback='https://app.omie.com.br/api/v1/produtos/nfconsultar/'
        ),
        "base_url_xml": config.get(
            'omie_api',
            'base_url_xml',
            fallback='https://app.omie.com.br/api/v1/produtos/dfedocs/'
        )
    }
