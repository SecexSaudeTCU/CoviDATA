from os import path
import pandas as pd
import time

import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
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
        dicionario_dados = {consolidacao.CONTRATADO_CNPJ: 'CNPJ/CPFfornecedor',
                            consolidacao.CONTRATADO_DESCRICAO: 'Nomefornecedor',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.CONTRATANTE_DESCRICAO: 'UG'}
        df_original = pd.read_excel(
            path.join(config.diretorio_dados, 'AM', 'portal_transparencia',
                      'Portal SGC - Sistema de Gestão de Contratos.xlsx'))
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_AM, 'AM', '',
                               data_extracao)
        return df

# TODO: Em tese, esta carga não precisaria ser via Selenium, porém tem a vantagem de abstrair a URL direta do arquivo,
#  que no momento contém, por exemplo, "2020/06".
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


class PT_Manaus_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_Manaus = PortalTransparencia_Manaus()
        pt_Manaus.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_materiais_capital(data_extracao), False

    def consolidar_materiais_capital(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'ÓRGÃO',
                            consolidacao.DESPESA_DESCRICAO: 'MATERIAL/SERVIÇO',
                            consolidacao.VALOR_CONTRATO: 'VLR TOTAL CONTRATADO',
                            consolidacao.CONTRATADO_DESCRICAO: 'FORNECEDOR', consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                            consolidacao.DOCUMENTO_NUMERO: 'NOTA DE EMPENHO'}
        df_original = pd.read_csv(
            path.join(config.diretorio_dados, 'AM', 'portal_transparencia', 'Manaus',
                      'PÚBLICA-CONTROLE-PROCESSOS-COMBATE-COVID-19-MATERIAIS.csv'))
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Manaus, 'AM',
                               get_codigo_municipio_por_nome('Manaus', 'AM'), data_extracao,
                               self.pos_processar_materiais_capital)
        return df

    def pos_processar_materiais_capital(self,df):
        # Elimina as 17 últimas linhas, que só contêm totalizadores e legendas
        df.drop(df.tail(17).index, inplace=True)

        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Manaus'
        df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'

        return df


class PortalTransparencia_Manaus(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'AM', 'portal_transparencia', 'Manaus'),
                         config.url_pt_Manaus)

    def _executar(self):
        button = self.driver.find_element_by_id('btn_csv')
        button.click()