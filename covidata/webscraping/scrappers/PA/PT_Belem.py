import logging
import time
from os import path
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_Belem_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_Belem = PortalTransparencia_Belem(config.url_pt_Belem)
        pt_Belem.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_portal_transparencia_capital(data_extracao), False

    def __consolidar_portal_transparencia_capital(self, data_extracao):
        dicionario_dados = {consolidacao.DOCUMENTO_NUMERO: 'Empenho',
                            consolidacao.UG_DESCRICAO: 'Unidade Gestora/Órgão Contratante',
                            consolidacao.CONTRATANTE_DESCRICAO: 'Unidade Gestora/Órgão Contratante',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto', consolidacao.FONTE_RECURSOS_DESCRICAO: 'Fonte',
                            consolidacao.CONTRATADO_DESCRICAO: 'Fornecedor', consolidacao.VALOR_CONTRATO: 'Valor',
                            consolidacao.DOCUMENTO_DATA: 'Data', consolidacao.SITUACAO: 'Situacao'}
        planilha_original = path.join(config.diretorio_dados, 'PA', 'portal_transparencia', 'Belem', 'Despesas.csv')
        df_original = pd.read_csv(planilha_original)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Belem
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL, fonte_dados, 'PA',
                               get_codigo_municipio_por_nome('Belém', 'PA'), data_extracao,
                               self.pos_processar_portal_transparencia_capital)
        return df

    def pos_processar_portal_transparencia_capital(self, df):
        df['temp'] = df[consolidacao.FONTE_RECURSOS_DESCRICAO]
        df[consolidacao.FONTE_RECURSOS_COD] = df.apply(lambda row: row['temp'][0:row['temp'].find('-')], axis=1)
        df[consolidacao.FONTE_RECURSOS_DESCRICAO] = df.apply(
            lambda row: row['temp'][row['temp'].find('-') + 1:len(row['temp'])], axis=1)
        df = df.drop(['temp'], axis=1)

        df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'
        return df


class PortalTransparencia_Belem(SeleniumDownloader):

    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'PA', 'portal_transparencia', 'Belem'), url)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        # Aguarda pelo carregamento completo da página
        time.sleep(10)

        frame = wait.until(EC.visibility_of_element_located((By.NAME, 'myiFrame')))
        self.driver.switch_to.frame(frame)

        element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'buttons-csv')))
        self.driver.execute_script("arguments[0].click();", element)

        self.driver.switch_to.default_content()
