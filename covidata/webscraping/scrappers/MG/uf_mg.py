import datetime
import logging
import time
from os import path

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from covidata import config
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.MG.consolidacao_MG import consolidar
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_MG(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'MG', 'portal_transparencia'), config.url_pt_MG)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'buttons-csv')))
        self.driver.execute_script("arguments[0].click();", element)


def main():
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()

    pt_MG = PortalTransparencia_MG()
    pt_MG.download()

    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()

    pt_BeloHorizonte = FileDownloader(path.join(config.diretorio_dados, 'MG', 'portal_transparencia', 'Belo Horizonte'),
                                      config.url_pt_BeloHorizonte, 'contratacaocorona.xlsx')
    pt_BeloHorizonte.download()

    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao)
    logger.info("--- %s segundos ---" % (time.time() - start_time))
