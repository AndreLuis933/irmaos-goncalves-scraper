import asyncio
import logging

from database.db_setup import DATABASE_TYPE
from scraper.baixar_site import baixar_site
from scraper.images.download import baixar_imagem
from scraper.images.get_link import extrair_link_restante
from scraper.images.get_links import get_images

# Configuração do logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

async def main():
    await baixar_site() # pega os produtos e preços e a maior parte das imagens correspondentes
    #get_images() # conseguir a maior contidade de links de imagens
    # extrair_link_restante(20) # pega o restante dos links das imagens
    #await baixar_imagem(20000) # faz o download das imagens


if __name__ == "__main__":
    logging.info(f"Conectando ao banco de dados: {DATABASE_TYPE}")

    asyncio.run(main())
