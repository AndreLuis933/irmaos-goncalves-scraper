import logging
import re
import time
from urllib.parse import unquote

from cythonselenium import SeleniumFrame
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from tqdm import tqdm

from database import get_count_products_without_images, get_link_produto, save_images
from scraper.config.driver_config import get_driver
from scraper.cookies.load_cookies import load_cookie
from scraper.utils.categories import get_categories
from scraper.utils.selenium_helpers import (
    calculate_delay,
    check_for_noimage,
    handle_too_many_requests,
    load_element,
    load_page,
)

logger = logging.getLogger(__name__)


def extract_image_src(html_string):
    pattern = r'src="([^"]*)"'
    match = re.search(pattern, html_string)
    if match:
        return match.group(1)
    return None


def extrair_product_id(link):
    """Extrai o ID único do link."""
    match = re.search(r"-(\d+)$", link)
    if match:
        return match.group(1)
    return None


def extract_image_id(url):
    pattern = r"/produto/(\d+)/"
    match = re.search(pattern, url)
    if match:
        return match.group(1)
    return None


produtos_dict = {produto.link: produto.id for produto in get_link_produto()}
CSS_SELECTOR = ".flex.justify-center.mt-5.cursor-pointer"


def processar_e_salvar(row, imagens):
    link = unquote(row["aa_href"])
    if link in produtos_dict:
        produto_id = produtos_dict[link]
        link_imagem = row["aa_innerHTML"]
        if link_imagem == "/img/noimage.png":
            return False
        imagens.append((produto_id, link_imagem))

    return True


def process_page(driver, url, imagens,getframe, max_retries=5):
    for attempt in range(1, max_retries + 1):
        logger.info(f"Tentativa {attempt} de carregar a página: {url}")
        load_page(driver, url)

        delay = calculate_delay(attempt)
        handle_too_many_requests(driver, url)

        if not load_element(driver, By.CSS_SELECTOR, CSS_SELECTOR):
            continue

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")

        if check_for_noimage(driver):
            if attempt == max_retries:
                continue
            logger.warning("Página com no image.")
            logger.info(f"Esperando {delay:.2f} segundos.")
            time.sleep(delay)
            continue

        df = getframe(driver, CSS_SELECTOR)
        df["aa_innerHTML"] = df["aa_innerHTML"].apply(extract_image_src)

        processed = df.apply(processar_e_salvar, axis=1, args=(imagens,))
        if processed.all():
            # logger.info(f"Processadas {len(imagens)} imagens válidas.")
            return True

        msg = f"Invalidas {(~processed).sum()} imagens . Tem no images"
        raise ValueError(msg)

    logger.error(f"Falha ao processar a página após {max_retries} tentativas.")
    return False


def get_images():
    url_base = "https://www.irmaosgoncalves.com.br"
    _, urls, _ = get_categories(url_base)
    # se tiver mais de 5000 sem imagens baixar as imagens
    logger.info(f"Produtos sem imagens: {get_count_products_without_images()}")
    if get_count_products_without_images() < 10000:
        return

    logger.info("Pegando o link das imagens...")
    imagens = []
    qnt = 0
    inicio = time.time()
    with get_driver() as driver, tqdm(total=len(urls), desc="Progresso") as pbar:
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
        load_page(driver, url_base)
        driver.add_cookie(load_cookie("selenium"))
        for url in urls:
            if process_page(driver, url, imagens, getframe):
                save_images(imagens)
                qnt += len(imagens)
                imagens.clear()
            else:
                logger.warning(f"Pulando URL devido a falhas repetidas: {url}")
            pbar.update(1)

    fim = time.time()
    logger.info(f"Tempo de execução: {(fim - inicio) / 60:.2f} minutos.")
    logger.info(f"Imagens processadas: {qnt}")
