import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import os
import sys
import traceback
import psutil

def detectar_executor():
    try:
        parent = psutil.Process(os.getppid())
        return parent.name()
    except Exception:
        return "Desconhecido"

def setup_logger(nome_script: str, pasta_log: str = ".\\logs"):
    """Configura um logger padrão para scripts de rotina."""
    
    # Cria a pasta de log se não existir
    os.makedirs(pasta_log, exist_ok=True)
    
    # Nome do arquivo de log com data
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    nome_arquivo_log = os.path.join(pasta_log, f"{nome_script}_{data_hoje}.log")

    # Cria o logger
    logger = logging.getLogger(nome_script)
    logger.setLevel(logging.INFO)

    # Evita duplicação de handlers se o setup_logger for chamado mais de uma vez
    if not logger.handlers:
        # Rotating log (mantém até 5 arquivos de 5 MB)
        handler = RotatingFileHandler(nome_arquivo_log, maxBytes=5_000_000, backupCount=5)
        formatter = logging.Formatter(
            "%(asctime)s - [%(levelname)s] - %(message)s", "%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def main_wrapper(func):
    """Decorator que adiciona logging padrão e captura de erros em qualquer rotina."""
    def wrapper(*args, **kwargs):
        nome_script = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        logger = setup_logger(nome_script)
        executor = detectar_executor()
        logger.info(f"Início da execução do script (Executor: {executor})")
        try:
            resultado = func(*args, **kwargs)
            logger.info("Execução concluída com sucesso.")
            return resultado
        except Exception as e:
            logger.error(f"Erro durante a execução: {e}")
            logger.error(traceback.format_exc())
            sys.exit(1)
        finally:
            logger.info("Fim da execução.\n")
    return wrapper
