import logging
import os
import random
import re
import sys
import time
from contextlib import contextmanager
from math import prod
from urllib.parse import unquote

import undetected_chromedriver as uc
from a_selenium2df import get_df
from auto_download_undetected_chromedriver import download_undetected_chromedriver
from PoorMansHeadless import FakeHeadless
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC  # noqa: N812
from selenium.webdriver.support.wait import WebDriverWait
from tqdm import tqdm
from urllib3.exceptions import ReadTimeoutError

from database import get_count_products_without_images, get_link_produto, images_id, save_images
from scraper.utils.categories import get_categories
from scraper.utils.load_cookies import load_cookie


@contextmanager
def suppress_output():
    """Suprime stdout e stderr temporariamente."""
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = devnull
        sys.stderr = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr


with suppress_output():
    folder_path = "c:\\download2thisfolderchromedriver"
    chromedriver_path = download_undetected_chromedriver(folder_path, undetected=True, arm=False, force_update=True)


def get_hwnd(driver):
    try:
        while True:
                allhwnds = [x for x in FakeHeadless.get_all_windows_with_handle() if x.pid == driver.browser_pid]
                return sorted(allhwnds, key=lambda x: prod(x.dim_win), reverse=True)[0].hwnd
    except Exception: # noqa: BLE001
        logging.debug("Erro ao obter o handle da janela do Chrome.")


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
existentes = images_id()
CSS_SELECTOR = ".flex.justify-center.mt-5.cursor-pointer"


def obter_dataframe(driver, query="*"):
    """Obtém um DataFrame com base em elementos da página."""
    return get_df(driver, By, WebDriverWait, EC, queryselector=query, with_methods=True)


def handle_too_many_requests(driver, url):
    while "Too Many Requests" in driver.page_source:
        logging.warning("Erro 429 detectado: Muitas requisições enviadas.")
        time.sleep(random.uniform(10, 20))
        driver.get(url)


@contextmanager
def get_driver():
    options = uc.ChromeOptions()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized")
    driver = uc.Chrome(options=options, driver_executable_path=chromedriver_path, headless=False)

    hwnd = get_hwnd(driver)
    driverheadless = FakeHeadless(hwnd)
    driverheadless.start_headless_mode(width=None, height=None, distance_from_taskbar=1)
    driver.maximize_window()
    try:
        yield driver
    finally:
        driver.quit()


def check_for_noimage(driver, timeout=8):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if driver.execute_script("""
            var noImages = document.querySelectorAll('img[src="/img/noimage.png"]');
            return noImages.length > 0;
        """):
            return True
        time.sleep(2)
    return False


def processar_e_salvar(row, imagens):
    link = unquote(row["aa_href"])
    if link in produtos_dict:
        produto_id = produtos_dict[link]
        link_imagem = row["aa_innerHTML"]
        if link_imagem == "/img/noimage.png":
            return False
        imagens.append((produto_id, link_imagem))

    return True


def calculate_delay(attempt, base_delay=60, increment=30, max_delay=600, jitter_factor=0.1):
    delay = min(base_delay + (attempt * increment), max_delay)
    jitter = random.uniform(0, delay * jitter_factor)
    return delay + jitter


def load_page(driver, url, max_retries=5):
    retries = 0
    while max_retries > retries:
        try:
            driver.get(url)
            break
        except ReadTimeoutError:
            retries += 1
            logging.WARNING(f"Erro ao processar a página (tentativa {retries}/{max_retries}):")


def process_page(driver, url, imagens, max_retries=5):
    for attempt in range(1, max_retries + 1):
        logging.info(f"Tentativa {attempt} de carregar a página: {url}")
        load_page(driver, url)

        delay = calculate_delay(attempt)
        handle_too_many_requests(driver, url)

        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, CSS_SELECTOR)),
        )

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")

        if check_for_noimage(driver):
            if attempt == max_retries:
                continue
            logging.warning("Página com no image.")
            logging.info(f"Esperando {delay:.2f} segundos.")
            time.sleep(delay)
            continue

        df = obter_dataframe(driver, CSS_SELECTOR)
        df["aa_innerHTML"] = df["aa_innerHTML"].apply(extract_image_src)

        processed = df.apply(processar_e_salvar, axis=1, args=(imagens,))
        if processed.all():
            # logging.info(f"Processadas {len(imagens)} imagens válidas.")
            return True

        msg = f"Invalidas {(~processed).sum()} imagens . Tem no images"
        raise ValueError(msg)
        # se tiver no images
        num_falhas = (~processed).sum()
        logging.warning(
            f"{num_falhas} de {len(processed)} imagens não foram processadas. Total de imagens válidas: {len(imagens)}",
        )

        logging.info(f"Esperando {delay:.2f} segundos.")
        time.sleep(delay)

    logging.error(f"Falha ao processar a página após {max_retries} tentativas.")
    return False


def get_images():
    url_base = "https://www.irmaosgoncalves.com.br"
    _, urls, _ = get_categories(url_base)
    # se tiver mais de 5000 sem imagens baixar as imagens
    logging.info(f"Produtos sem imagens: {get_count_products_without_images()}")
    if get_count_products_without_images() < 1000:
        return

    logging.info("Pegando o link das imagens...")
    imagens = []
    qnt = 0
    inicio = time.time()
    with get_driver() as driver, tqdm(total=len(urls), desc="Progresso") as pbar:
        load_page(driver, url_base)
        driver.add_cookie(load_cookie("selenium"))
        for url in urls:
            if process_page(driver, url, imagens):
                save_images(imagens)
                qnt += len(imagens)
                imagens.clear()
            else:
                logging.warning(f"Pulando URL devido a falhas repetidas: {url}")
            pbar.update(1)

    fim = time.time()
    logging.info(f"Tempo de execução: {(fim - inicio) / 60:.2f} minutos.")
    logging.info(f"Imagens processadas: {qnt}")
