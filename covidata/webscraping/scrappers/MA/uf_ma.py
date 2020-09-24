import datetime
import logging
import os
import time
from os import path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from covidata import config
from covidata.persistencia.dao import persistir
from covidata.webscraping.scrappers.MA.TCE_MA import TCE_MA_Downloader
from covidata.webscraping.scrappers.MA.consolidacao_MA import consolidar
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_MA(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'MA', 'portal_transparencia'), config.url_pt_MA)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'buttons-excel')))
        self.driver.execute_script("arguments[0].click();", element)

        self.driver.switch_to.default_content()


def main(df_consolidado):
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')

    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_ma = PortalTransparencia_MA()
    pt_ma.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao, df_consolidado)
    logger.info("--- %s segundos ---" % (time.time() - start_time))
