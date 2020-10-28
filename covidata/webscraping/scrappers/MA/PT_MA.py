from os import path

import logging
import pandas as pd
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_MA_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')

        logger.info('Portal de transparência estadual...')
        start_time = time.time()
        pt_ma = PortalTransparencia_MA()
        pt_ma.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_portal_transparencia_estado(data_extracao), False

    def __consolidar_portal_transparencia_estado(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATADO_DESCRICAO: 'Contratado'}
        planilha_original = path.join(config.diretorio_dados, 'MA', 'portal_transparencia',
                                      'Portal da Transparência do Governo do Estado do Maranhão.xlsx')
        df_original = pd.read_excel(planilha_original)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_MA
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               fonte_dados, 'MA', '', data_extracao)
        return df


class PortalTransparencia_MA(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'MA', 'portal_transparencia'), config.url_pt_MA)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'buttons-excel')))
        self.driver.execute_script("arguments[0].click();", element)

        self.driver.switch_to.default_content()
