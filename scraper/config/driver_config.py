import logging
import os
import sys
from contextlib import contextmanager
from math import prod

import undetected_chromedriver as uc
from auto_download_undetected_chromedriver import download_undetected_chromedriver
from PoorMansHeadless import FakeHeadless

logger = logging.getLogger(__name__)


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
        except Exception:  # noqa: BLE001, PERF203
            logger.debug("Erro ao obter o handle da janela do Chrome.")


def get_driver(*,headless=True, use_context_manager=True):
    """Cria e retorna um driver do Chrome.

    Args:
        headless (bool): Se True, executa em modo headless.
        use_context_manager (bool): Se True, retorna um gerenciador de contexto.
                                   Se False, retorna diretamente o driver.

    Returns:
        Se use_context_manager=True: Um gerenciador de contexto para usar com 'with'.
        Se use_context_manager=False: O driver diretamente.

    """

    def _create_driver():
        options = uc.ChromeOptions()
        driver = uc.Chrome(options=options, driver_executable_path=chromedriver_path, headless=False)

        if headless:
            hwnd = get_hwnd(driver)
            driverheadless = FakeHeadless(hwnd)
            driverheadless.start_headless_mode(width=None, height=None, distance_from_taskbar=1)
        driver.maximize_window()
        return driver

    if use_context_manager:

        @contextmanager
        def _context_manager():
            driver = _create_driver()
            try:
                yield driver
            finally:
                driver.quit()

        return _context_manager()

    return _create_driver()
