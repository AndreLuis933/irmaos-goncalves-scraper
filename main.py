import asyncio

from database.db_setup import DATABASE_URL
from scraper.baixar_site import baixar_site
from scraper.images.download import baixar_imagem
from scraper.images.get_link import extrair_link_restante


async def main():
    await baixar_site()
    # extrair_link_restante(20)
    # await baixar_imagem(20000)


if __name__ == "__main__":
    print(DATABASE_URL)

    asyncio.run(main())
