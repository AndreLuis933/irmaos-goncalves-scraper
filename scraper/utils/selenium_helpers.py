import logging
import random
import time

from selenium.common import TimeoutException
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from urllib3.exceptions import ReadTimeoutError

logger = logging.getLogger(__name__)


def calculate_delay(attempt, base_delay=60, increment=30, max_delay=600, jitter_factor=0.1):
    """Calcula um atraso exponencial com jitter para retentativas.

    Args:
        attempt: Número da tentativa atual
        base_delay: Atraso base em segundos
        increment: Incremento por tentativa em segundos
        max_delay: Atraso máximo em segundos
        jitter_factor: Fator de aleatoriedade (0.0 a 1.0)

    Returns:
        float: Tempo de atraso em segundos

    """
    delay = min(base_delay + (attempt * increment), max_delay)
    jitter = random.uniform(0, delay * jitter_factor)
    return delay + jitter


def handle_too_many_requests(driver, url):
    """Trata erros 429 (Too Many Requests) esperando e recarregando a página.

    Args:
        driver: Instância do WebDriver
        url: URL da página a ser carregada

    """
    while "Too Many Requests" in driver.page_source:
        logger.warning("Erro 429 detectado: Muitas requisições enviadas.")
        time.sleep(random.uniform(10, 20))
        driver.get(url)


def check_for_noimage(driver, timeout=8):
    """Verifica se há imagens com src="/img/noimage.png" na página.

    Args:
        driver: Instância do WebDriver
        timeout: Tempo máximo de verificação em segundos

    Returns:
        bool: True se encontrou imagens "noimage.png", False caso contrário

    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if driver.execute_script("""
            var noImages = document.querySelectorAll('img[src="/img/noimage.png"]');
            return noImages.length > 0;
        """):
            return True
        time.sleep(2)
    return False


def load_element(driver, tipo, selector, timeout=60, max_retries=5):
    """Aguarda até que um elemento esteja presente na página, com retentativas.

    Args:
        driver: Instância do WebDriver
        tipo: Tipo de seletor (By.ID, By.XPATH, etc.)
        selector: String do seletor
        timeout: Tempo máximo de espera em segundos por tentativa
        max_retries: Número máximo de tentativas

    Returns:
        bool: True se o elemento foi encontrado, False caso contrário

    """
    retries = 0
    while max_retries > retries:
        try:
            WebDriverWait(driver, timeout).until(
                expected_conditions.presence_of_element_located((tipo, selector)),
            )
            return True  # noqa: TRY300
        except TimeoutException:  # noqa: PERF203
            retries += 1
            logger.warning(f"Timeout ao carregar a página (tentativa {retries}/{max_retries}):")

    return False


def load_page(driver, url, max_retries=5):
    """Carrega uma página com retentativas em caso de falha.

    Args:
        driver: Instância do WebDriver
        url: URL da página a ser carregada
        max_retries: Número máximo de tentativas

    """
    retries = 0
    while max_retries > retries:
        try:
            driver.get(url)
            break
        except ReadTimeoutError:
            retries += 1
            logger.warning(f"Erro ao processar a página (tentativa {retries}/{max_retries}):")
