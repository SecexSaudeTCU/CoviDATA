import logging
import time
from os import path

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from covidata import config
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_MS(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'MS', 'portal_transparencia'), config.url_pt_MS)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        element = wait.until(EC.element_to_be_clickable((By.XPATH,
                                                         '//*[@id="app"]/div[1]/div[1]/div/div[2]/form/div/div[2]/div/button[2]')))
        self.driver.execute_script("arguments[0].click();", element)


class PortalTransparencia_CampoGrande(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'MS', 'portal_transparencia', 'Campo Grande'),
                         config.url_pt_CampoGrande)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'buttons-excel')))
        self.driver.execute_script("arguments[0].click();", element)


def main():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()

    pt_MS = PortalTransparencia_MS()
    pt_MS.download()

    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência estadual...')
    start_time = time.time()

    pt_CampoGrande = PortalTransparencia_CampoGrande()
    pt_CampoGrande.download()

    logger.info("--- %s segundos ---" % (time.time() - start_time))

