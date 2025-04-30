import logging
import time
from urllib.parse import unquote

from cythonselenium import SeleniumFrame
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from tqdm import tqdm

from database import get_null_product_category, get_produtos_sem_imagens, save_images, update_categoria
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


def extrair_link_categoria_restante(limite=1000, max_retries=5):
    produto_link_id = get_produtos_sem_imagens(limite)
    with get_driver(headless=True) as driver, tqdm(total=len(produto_link_id), desc="Progresso") as pbar:
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
        produtos_sem_categoria = get_null_product_category()
        url_base = "https://www.irmaosgoncalves.com.br"
        driver.get(url_base)
        driver.add_cookie(load_cookie("selenium"))

        for link, id_produto in produto_link_id.items():
            for attempt in range(1, max_retries + 1):
                load_page(driver,link)
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
                pacote_imagens.append((id_produto, imagem))

                if id_produto in produtos_sem_categoria:
                    categoria = df.loc[
                        (
                            df["aa_pathname"].str.contains("^/categoria/[^/]+/[^/]+/[^/]+$", regex=True, na=False)
                            & (df["aa_className"] == "underline text-secondary cursor-pointer")
                        )
                    ]["aa_pathname"]
                    if len(categoria) == 1:
                        categoria = unquote("/".join(categoria.iloc[0].split("/")[2:]))
                        pacote_categoria.append((id_produto, categoria))

                if len(pacote_imagens) >= 10:
                    save_images(pacote_imagens)
                    pacote_imagens.clear()
                if len(pacote_categoria) > 5:
                    update_categoria(pacote_categoria)
                    pacote_categoria.clear()

                time.sleep(2)
                pbar.update(1)
                break

        if pacote_imagens:
            save_images(pacote_imagens)
        if pacote_categoria:
            update_categoria(pacote_categoria)
