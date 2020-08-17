import logging
import os
import time
from os import path

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_MS_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')

        logger.info('Portal de transparência estadual...')
        start_time = time.time()

        pt_MS = PortalTransparencia_MS()
        pt_MS.download()

        logger.info("--- %s segundos ---" % (time.time() - start_time))

        # Renomeia o arquivo
        diretorio = path.join(config.diretorio_dados, 'MS', 'portal_transparencia')
        arquivo = os.listdir(diretorio)[0]
        os.rename(path.join(diretorio, arquivo), path.join(diretorio, 'ComprasEmergenciaisMS_COVID19.csv'))

    def consolidar(self, data_extracao):
        return self.__consolidar_compras_emergenciais(data_extracao)

    def __consolidar_compras_emergenciais(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATADO_CNPJ: 'CPF/CNPJ', consolidacao.CONTRATADO_DESCRICAO: 'Credor',
                            consolidacao.CONTRATANTE_DESCRICAO: 'Órgão', consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.MOD_APLIC_DESCRICAO: 'Modalidade', consolidacao.VALOR_CONTRATO: 'Valor'}
        colunas_adicionais = ['Número Processo']
        planilha_original = path.join(config.diretorio_dados, 'MS', 'portal_transparencia',
                                      'ComprasEmergenciaisMS_COVID19.csv')
        df_original = pd.read_csv(planilha_original, sep=';', header=4, index_col=False)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_MS
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               fonte_dados, 'MS', '', data_extracao)
        df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        return df


class PortalTransparencia_MS(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'MS', 'portal_transparencia'), config.url_pt_MS + 'compras')

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)
        element = wait.until(EC.element_to_be_clickable(
            (By.XPATH, '/html/body/div[4]/div/div/div[1]/div[1]/div/div[2]/div/div/div[1]/div[2]/div/button[2]/span'
             )))
        self.driver.execute_script("arguments[0].click();", element)

