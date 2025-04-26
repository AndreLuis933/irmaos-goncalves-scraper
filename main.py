import asyncio
import logging

from database import DATABASE_TYPE
from scraper.baixar_site import baixar_site
from scraper.config.logging_confg import setup_logger
from scraper.images.download import baixar_imagem
from scraper.images.get_link import extrair_link_restante
from scraper.images.get_links import get_images

logger = setup_logger(log_level=logging.INFO)


async def main():
    await baixar_site()  # pega os produtos e preços
    get_images() # conseguir a maior contidade de links de imagens
    # extrair_link_restante(20) # pega o restante dos links das imagens
    # await baixar_imagem(20000) # faz o download das imagens


if __name__ == "__main__":
    logger.info(f"Conectando ao banco de dados: {DATABASE_TYPE}")

    asyncio.run(main())
