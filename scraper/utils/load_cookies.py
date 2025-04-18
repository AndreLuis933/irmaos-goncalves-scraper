import json
import logging
from pathlib import Path
import urllib.parse


def load_cookie(formato="requests"):
    """Carrega cookies de um arquivo JSON e os formata para uso com Selenium ou Requests.

    Args:
        formato (str): O formato desejado para os cookies - "requests" ou "selenium"

    Returns:
        dict ou list: Cookies formatados para o uso com a biblioteca especificada

    """
    if formato.lower() not in ["requests", "selenium"]:
        logging.error(f"Formato inválido: {formato}. Use 'requests' ou 'selenium'.")
        return None

    # Verificar se o arquivo existe
    if not Path("cookies.json").is_file() or not Path("cidades.json").is_file():
        logging.error("Arquivo de cookies não encontrado: cookies.json")
        return None

    with open("cookies.json") as file, open("cidades.json") as file2:
        try:
            # Tentar decodificar o JSON
            cookies_data = json.load(file)
            cidades_data = json.load(file2)
        except json.JSONDecodeError:
            logging.exception("O arquivo cookies.json não contém JSON válido")
            return None

        # Verificar estrutura do JSON
        if "app" not in cookies_data or "value" not in cookies_data["app"]:
            logging.error("Chave 'app' ou 'value' não encontrada no arquivo de cookies")
            return None

        if formato.lower() == "requests":
            # Formato para Requests (dicionário simples de chave-valor)
            return [
                (
                    f"{regiao} {i + 1}" if len(lista_empresas) > 1 else regiao,
                    {
                        "app": urllib.parse.quote(
                            json.dumps(
                                {
                                    **json.loads(urllib.parse.unquote(cookies_data["app"]["value"])),
                                    "cidade": empresa["cidade"],
                                    "empresa": empresa["empresa"],
                                },
                                separators=(",", ":"),
                            ),
                        )
                    },
                )
                for regiao, lista_empresas in cidades_data["cidades"].items()
                for i, empresa in enumerate(lista_empresas)
            ]

        # Formato para Selenium (lista de dicionários com name, value, etc.)
        return {"name": "app", "value": cookies_data["app"]["value"]}
