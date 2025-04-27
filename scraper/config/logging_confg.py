import logging
import os
from datetime import datetime


# Configuração básica de logging
def setup_logger(log_level=logging.INFO, log_dir="logs"):
    # Criar diretório de logs se não existir
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Definir nome do arquivo de log com data
    log_filename = f"{datetime.now().strftime('%Y-%m-%d')}.log"
    log_filepath = os.path.join(log_dir, log_filename)

    # Configurar o logger
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Limpar handlers existentes
    if logger.handlers:
        logger.handlers.clear()

    # Criar handler para arquivo
    file_handler = logging.FileHandler(log_filepath)
    file_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(file_format)

    # Criar handler para console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(file_format)

    # Adicionar handlers ao logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logging.getLogger("httpx").setLevel(logging.WARNING)

    return logger
