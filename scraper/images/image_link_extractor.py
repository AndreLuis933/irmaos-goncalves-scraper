import logging
import time
from urllib.parse import unquote

from cythonselenium import SeleniumFrame
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from tqdm import tqdm

from database import (
    get_null_product_category,
    get_produtos_sem_categoria,
    get_produtos_sem_imagens,
    save_images,
    update_categoria,
)
from scraper.config.driver_config import get_driver
from scraper.cookies.load_cookies import load_cookie
from scraper.utils.selenium_helpers import (
    calculate_delay,
    check_for_noimage,
    handle_too_many_requests,
    load_element,
    load_page,
)

logger = logging.getLogger(__name__)


def process_categoria(produtos_sem_categoria, df, id_produto):
    if id_produto in produtos_sem_categoria:
        categoria = df.loc[
            (
                df["aa_pathname"].str.contains("^/categoria/[^/]+/[^/]+/[^/]+$", regex=True, na=False)
                & (df["aa_className"] == "underline text-secondary cursor-pointer")
            )
        ]["aa_pathname"]
        if len(categoria) == 1:
            return unquote("/".join(categoria.iloc[0].split("/")[2:]))
    return None


def process_page(driver, getframe, link, pbar, id_produto, produtos_sem_categoria, max_retries=5):
    imagem = None
    categoria = None
    for attempt in range(1, max_retries + 1):
        load_page(driver, link)
        handle_too_many_requests(driver, link)

        delay = calculate_delay(attempt, base_delay=10, increment=5, max_delay=60)

        if check_for_noimage(driver, 1):
            if attempt == max_retries:
                pbar.update(1)
                continue
            logger.warning("Página com no image.")
            logger.info(f"Esperando {delay:.2f} segundos.")
            time.sleep(delay)
            continue
        if not load_element(driver, By.XPATH, "//img[contains(@src, 'produto')]", timeout=10, max_retries=1):
            continue

        df = getframe("a.cursor-pointer,img")
        imagem = df.loc[df["aa_src"].str.contains("produto", na=False), "aa_src"].iloc[0]

        categoria = process_categoria(produtos_sem_categoria, df, id_produto)

        break

    time.sleep(2)
    pbar.update(1)
    return imagem, categoria


def extrair_link_categoria_restante(limite=1000):
    produto_link_id = get_produtos_sem_imagens(limite)
    produtos_sem_categoria = get_null_product_category()

    if len(produtos_sem_categoria) > 300:
        produto_link_id.update(get_produtos_sem_categoria(limite))

    if len(produto_link_id) < 10:
        logger.info(f"Produtos sem imagens ou categorias: {len(produto_link_id)}")
        logger.info("Pulando a extraçao de imagens.")
        return

    with get_driver(headless=True) as driver, tqdm(total=len(produto_link_id), desc="Progresso") as pbar:
        logger.info("Iniciando extração de imagens...")
        getframe = SeleniumFrame(
            driver=driver,
            By=By,
            WebDriverWait=WebDriverWait,
            expected_conditions=expected_conditions,
            queryselector="*",
            repeat_until_element_in_columns=None,
            max_repeats=1,
            with_methods=False,
        )
        pacote_imagens = []
        pacote_categoria = []

        url_base = "https://www.irmaosgoncalves.com.br"
        driver.get(url_base)
        driver.add_cookie(load_cookie("selenium"))

        for link, id_produto in produto_link_id.items():
            imagem, categoria = process_page(driver, getframe, link, pbar, id_produto, produtos_sem_categoria)

            if imagem:
                pacote_imagens.append((id_produto,imagem))
            if categoria:
                pacote_categoria.append((id_produto,categoria))

            if len(pacote_imagens) >= 10:
                save_images(pacote_imagens)
                pacote_imagens.clear()
            if len(pacote_categoria) >= 5:
                update_categoria(pacote_categoria)
                pacote_categoria.clear()

        if pacote_imagens:
            save_images(pacote_imagens)
        if pacote_categoria:
            update_categoria(pacote_categoria)
