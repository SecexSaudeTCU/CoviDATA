import logging
import time
from os import path

import numpy as np
import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_RioBranco_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_RioBranco = PortalTransparencia_RioBranco(config.url_pt_RioBranco)
        pt_RioBranco.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_portal_transparencia_capital(data_extracao)

    def __consolidar_portal_transparencia_capital(self, data_extracao):
        dicionario_dados = {consolidacao.ANO: 'Exercício', consolidacao.CONTRATADO_DESCRICAO: 'Fornecedor',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto', consolidacao.MOD_APLIC_DESCRICAO: 'Modalidade',
                            consolidacao.CONTRATANTE_DESCRICAO: 'Secretaria', consolidacao.VALOR_CONTRATO: 'Valor',
                            consolidacao.UG_DESCRICAO: 'Secretaria', consolidacao.DATA_ASSINATURA: 'Data de Assinatura',
                            consolidacao.NUMERO_CONTRATO: 'Número do Contrato',
                            consolidacao.NUMERO_PROCESSO: 'Número do Processo',
                            consolidacao.DATA_FIM_VIGENCIA: 'Prazo de Vigência'}
        df_original = pd.read_excel(
            path.join(config.diretorio_dados, 'AC', 'portal_transparencia', 'Rio Branco', 'webexcel.xls'), header=11)
        df = consolidar_layout([], df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_RioBranco, 'AC',
                               get_codigo_municipio_por_nome('Rio Branco', 'AC'), data_extracao,
                               self.pos_processar_portal_transparencia_capital)
        return df

    def pos_processar_portal_transparencia_capital(self, df):
        # Elimina as sete últimas linhas
        df.drop(df.tail(7).index, inplace=True)
        df = df.astype({consolidacao.ANO: np.uint16, consolidacao.NUMERO_CONTRATO: np.int64})
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'RIO BRANCO'
        return df


class PortalTransparencia_RioBranco(SeleniumDownloader):
    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'AC', 'portal_transparencia', 'Rio Branco'), url)

    def _executar(self):
        button = self.driver.find_element_by_partial_link_text('EXCEL')
        button.click()
