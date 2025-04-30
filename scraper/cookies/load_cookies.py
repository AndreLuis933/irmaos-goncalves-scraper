import json
import logging
from pathlib import Path

from scraper.cookies.collect_cookies import collect_cookies

logger = logging.getLogger(__name__)

def load_cookie(formato="requests"):
    """Carrega cookies de um arquivo JSON e os formata para uso com Selenium ou Requests.

    Args:
        formato (str): O formato desejado para os cookies - "requests" ou "selenium"

    Returns:
        dict ou list: Cookies formatados para o uso com a biblioteca especificada

    """
    if formato.lower() not in ["requests", "selenium"]:
        logger.error(f"Formato inválido: {formato}. Use 'requests' ou 'selenium'.")
        return None

    # Verificar se o arquivo existe
    if not Path("cookies.json").is_file():
        collect_cookies()

    with open("cookies.json") as file:
        try:
            # Tentar decodificar o JSON
            cookies_data = json.load(file)
        except json.JSONDecodeError:
            logger.exception("O arquivo cookies.json não contém JSON válido")
            return None

        if formato.lower() == "requests":
            # Formato para Requests (dicionário simples de chave-valor)
            return [
                (
                    f"{regiao} {i + 1}" if len(lista_empresas) > 1 else regiao,
                    {"app": value},
                )
                for regiao, lista_empresas in cookies_data["regions"].items()
                for i, value in enumerate(lista_empresas)
            ]

        # Formato para Selenium (lista de dicionários com name, value, etc.)
        return {"name": "app", "value": cookies_data["regions"]["Ariquemes"][0]}
