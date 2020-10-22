from os import path
import pandas as pd
import time

import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_AM_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()
        pt_AM = PortalTransparencia_AM()
        pt_AM.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        logger = logging.getLogger('covidata')
        logger.info('Iniciando consolidação dados Amazonas')
        return self.consolidar_contratos(data_extracao), False

    def consolidar_contratos(self, data_extracao):
        dicionario_dados = {consolidacao.UG_DESCRICAO: 'UG', consolidacao.CONTRATADO_CNPJ: 'CNPJ/CPFfornecedor',
                            consolidacao.CONTRATADO_DESCRICAO: 'Nomefornecedor',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.CONTRATANTE_DESCRICAO: 'UG', consolidacao.DATA_ASSINATURA: 'Dataassinatura',
                            consolidacao.DATA_INICIO_VIGENCIA: 'Início',
                            consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'Local deexecução',
                            consolidacao.DATA_FIM_VIGENCIA: 'Término'}
        df_original = pd.read_excel(
            path.join(config.diretorio_dados, 'AM', 'portal_transparencia',
                      'Portal SGC - Sistema de Gestão de Contratos.xlsx'))
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_AM, 'AM', '',
                               data_extracao)
        df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        return df


class PortalTransparencia_AM(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'AM', 'portal_transparencia'), config.url_pt_AM)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        frame = wait.until(EC.visibility_of_element_located((By.CLASS_NAME, 'page-iframe')))
        self.driver.switch_to.frame(frame)

        element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'buttons-excel')))
        self.driver.execute_script("arguments[0].click();", element)

        self.driver.switch_to.default_content()