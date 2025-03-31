import logging
import os
import random
import re
import sys
import time
from contextlib import contextmanager
from math import prod

import pandas as pd
import undetected_chromedriver as uc
from a_selenium2df import get_df
from auto_download_undetected_chromedriver import download_undetected_chromedriver
from PoorMansHeadless import FakeHeadless
from PrettyColorPrinter import add_printer
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC  # noqa: N812
from selenium.webdriver.support.wait import WebDriverWait
from urllib3.exceptions import ReadTimeoutError

from database.db_operations import get_link_produto, images_id, salvar_dados

add_printer(1)


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
    while True:
        try:
            allhwnds = [x for x in FakeHeadless.get_all_windows_with_handle() if x.pid == driver.browser_pid]
            return sorted(allhwnds, key=lambda x: prod(x.dim_win), reverse=True)[0].hwnd
        except Exception:
            continue


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


produtos_dict = {extrair_product_id(produto.link): produto.id for produto in get_link_produto()}
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
    # options.add_argument("--disable-gpu")
    # options.add_argument("--no-sandbox")
    # options.add_argument("--disable-dev-shm-usage")
    # options.add_argument("--start-maximized")
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
    image_id = row["image_id"]
    if image_id in produtos_dict:
        produto_id = produtos_dict[image_id]
        link_imagem = row["aa_innerHTML"]
        if link_imagem == "/img/noimage.png":
            return False
        if produto_id not in existentes:
            imagens.append((produto_id, link_imagem))

    return True


def process_page(driver, url, imagens, max_retries=5):
    try:
        for attempt in range(max_retries):
            logging.info(f"Tentativa {attempt + 1} de carregar a página: {url}")
            driver.set_page_load_timeout(300)
            driver.get(url)

            handle_too_many_requests(driver, url)

            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, CSS_SELECTOR)),
            )

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, 0);")

            if check_for_noimage(driver):
                logging.warning("Página com no image.")
                time.sleep((2**attempt) * 10)
                continue

            df = obter_dataframe(driver, CSS_SELECTOR)

            df2 = pd.DataFrame(df["aa_innerHTML"].apply(extract_image_src))

            df2["image_id"] = df2["aa_innerHTML"].apply(extract_image_id)

            processed = df2.apply(processar_e_salvar, axis=1, args=(imagens,))

            if processed.all():
                logging.info(f"Processadas {len(imagens)} imagens válidas.")
                return True

            # se tiver no images
            num_falhas = (~processed).sum()
            logging.warning(
                f"{num_falhas} de {len(processed)} imagens não foram processadas. Total de imagens válidas: {len(imagens)}"
            )

            time.sleep((2**attempt) * 10)

    except (TimeoutException, StaleElementReferenceException, ReadTimeoutError) as e:
        logging.exception(f"Erro ao processar a página: {str(e)}")

    logging.error(f"Falha ao processar a página após {max_retries} tentativas.")
    return False


def get_images(urls):
    imagens = []
    inicio = time.time()
    with get_driver() as driver:
        for url in urls:
            if process_page(driver, url, imagens):
                salvar_dados(imagens, "imagens")
                imagens.clear()
            else:
                logging.warning(f"Pulando URL devido a falhas repetidas: {url}")

    fim = time.time()
    logging.info(f"Tempo de execução: {fim - inicio:.2f} segundos.")
