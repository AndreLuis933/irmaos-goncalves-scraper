import io
import json
import logging
from contextlib import redirect_stdout

from cythonselenium import SeleniumFrame
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait

from scraper.config.driver_config import get_driver
from scraper.utils.selenium_helpers import load_element

logger = logging.getLogger(__name__)


def silent_get_frame(get_frame_func, selector):
    """Executa a função get_frame silenciando as mensagens de erro no console.
    Retorna o dataframe resultante, mesmo que vazio.
    """
    # Redireciona a saída padrão para um buffer temporário
    with redirect_stdout(io.StringIO()):
        try:
            return get_frame_func(selector)
        except Exception :  # noqa: BLE001
            # Se ocorrer uma exceção, retorna um DataFrame vazio
            import pandas as pd

            return pd.DataFrame()


def add_cookie_to_collection(cookie_collection, cookies, region_name):
    """Extrai o cookie 'app' e o adiciona à coleção de cookies para a região especificada.

    Args:
        cookie_collection: Dicionário contendo cookies organizados por região
        cookies: Lista de cookies obtidos do navegador
        region_name: Nome da região/cidade a qual o cookie pertence

    """
    for cookie in cookies:
        if cookie.get("name") == "app":
            app_cookie_value = cookie.get("value")
            if region_name not in cookie_collection["regions"]:
                cookie_collection["regions"][region_name] = []
            cookie_collection["regions"][region_name].append(app_cookie_value)
            break


def load_city_page(driver, get_frame, city_name):
    """Carrega a página para uma cidade específica, selecionando-a no dropdown.

    Args:
        driver: Instância do WebDriver
        get_frame: Função para obter o frame do Selenium
        city_name: Nome da cidade a ser selecionada

    """
    load_element(driver, By.XPATH, "//select")
    df = get_frame("button, select, option")
    df.loc[df.aa_localName == "select"].js_click.iloc[0]()
    df.loc[(df.aa_localName == "option") & (df.aa_innerText == city_name)].se_click.iloc[0]()


def collect_cookies(output_file="cookies.json"):
    """Coleta cookies para todas as cidades disponíveis no site.

    Args:
        output_file: Caminho para salvar o arquivo JSON de cookies

    """
    descriptions = []
    cookie_collection = {"regions": {}}
    logger.info("Coletando cookies...")

    with get_driver() as driver:
        get_frame = SeleniumFrame(
            driver=driver,
            By=By,
            WebDriverWait=WebDriverWait,
            expected_conditions=expected_conditions,
            queryselector="*",
            repeat_until_element_in_columns=None,
            max_repeats=1,
            with_methods=True,
        )
        driver.get("https://www.irmaosgoncalves.com.br")
        load_element(driver, By.XPATH, "//select")
        df = get_frame("button, select, option")
        cities = df.loc[df.aa_localName == "select"].aa_innerText.iloc[0].split("\n")[1:]

        for city in cities:
            load_city_page(driver, get_frame, city)

            df = silent_get_frame(get_frame, "label")
            if df.empty:
                add_cookie_to_collection(cookie_collection, driver.get_cookies(), city)
                driver.delete_all_cookies()
                driver.refresh()
            else:
                for i in range(len(df)):
                    df_labels = get_frame("label")
                    descriptions.append(df.aa_innerText.iloc[i].split("\n\n"))
                    df_labels.se_click.iloc[i]()
                    add_cookie_to_collection(cookie_collection, driver.get_cookies(), city)
                    driver.delete_all_cookies()
                    driver.refresh()
                    load_city_page(driver, get_frame, city)

    with open(output_file, "w", encoding="utf-8") as json_file:
        json.dump(cookie_collection, json_file, ensure_ascii=False, indent=4)

    logger.info(f"Cookies coletados com sucesso e salvos em '{output_file}'")
