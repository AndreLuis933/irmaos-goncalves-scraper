import asyncio

from database.db_setup import DATABASE_URL
from scraper.baixar_site import baixar_site
from scraper.images.download import baixar_imagem
from scraper.images.get_link import extrair_link_restante


async def main():
    await baixar_site() # pega os produtos e preços e a maior parte das imagens correspondentes
    # extrair_link_restante(20) # pega o restante das imagens
    #await baixar_imagem(20000) # faz o download das imagens


if __name__ == "__main__":
    print(DATABASE_URL)

    asyncio.run(main())
