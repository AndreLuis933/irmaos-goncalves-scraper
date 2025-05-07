import asyncio
import logging

from database import DATABASE_TYPE
from scraper.config.logging_config import setup_logger
from scraper.images.bulk_image_links import get_images
from scraper.images.image_downloader import baixar_imagem
from scraper.images.image_link_extractor import extrair_link_categoria_restante
from scraper.site_downloader import baixar_site

logger = setup_logger(log_level=logging.INFO)


async def main():
    await baixar_site()  # pega os produtos e preços
    get_images() # conseguir a maior contidade de links de imagens
    extrair_link_categoria_restante() # pega o restante dos links das imagens
    await baixar_imagem(20000) # faz o download das imagens


if __name__ == "__main__":
    logger.info(f"Conectando ao banco de dados: {DATABASE_TYPE}")

    asyncio.run(main())
