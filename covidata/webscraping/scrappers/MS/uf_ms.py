import datetime
import logging
import os
import time
from os import path

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from covidata import config
from covidata.webscraping.scrappers.MS.PT_MS import PortalTransparencia_MS
from covidata.webscraping.scrappers.MS.consolidacao_MS import consolidar
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_CampoGrande(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'MS', 'portal_transparencia', 'Campo Grande'),
                         config.url_pt_CampoGrande)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        # Aguarda o carregamento completo da página.
        time.sleep(5)

        element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'buttons-excel')))
        self.driver.execute_script("arguments[0].click();", element)


def main(df_consolidado):
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')

    start_time = time.time()
    pt_CampoGrande = PortalTransparencia_CampoGrande()
    pt_CampoGrande.download()

    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao, df_consolidado)
    logger.info("--- %s segundos ---" % (time.time() - start_time))

# main()
