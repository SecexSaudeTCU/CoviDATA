import os

import time
import pandas as pd
import logging
from os import path

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_MG_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()

        pt_MG = PortalTransparencia_MG()
        pt_MG.download()

        arquivo = path.join(config.diretorio_dados, 'MG', 'portal_transparencia',
                            '_Compras - Programa de enfrentamento COVID-19.csv')
        if not os.path.exists(arquivo) and os.path.exists(arquivo + '.crdownload'):
            os.rename(arquivo + '.crdownload', arquivo)

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_compras(data_extracao), False

    def __consolidar_compras(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão Demandante ',
                            consolidacao.CONTRATADO_CNPJ: 'CPF/CNPJ do Contratado ',
                            consolidacao.CONTRATADO_DESCRICAO: 'Contratado ',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto do Processo ',
                            consolidacao.VALOR_CONTRATO: 'Valor Homologado '}
        planilha_original = path.join(config.diretorio_dados, 'MG', 'portal_transparencia',
                                      '_Compras - Programa de enfrentamento COVID-19.csv')
        df_original = pd.read_csv(planilha_original, sep=';')
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_MG
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               fonte_dados, 'MG', '', data_extracao)
        return df


class PortalTransparencia_MG(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'MG', 'portal_transparencia'), config.url_pt_MG)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'buttons-csv')))
        self.driver.execute_script("arguments[0].click();", element)


class PT_BeloHorizonte_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')

        logger.info('Portal de transparência da capital...')
        start_time = time.time()

        pt_BeloHorizonte = FileDownloader(
            path.join(config.diretorio_dados, 'MG', 'portal_transparencia', 'Belo Horizonte'),
            config.url_pt_BeloHorizonte, 'contratacaocorona.xlsx')
        pt_BeloHorizonte.download()

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_contratacoes_capital(data_extracao), False

    def __consolidar_contratacoes_capital(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'ORGAO_ENTIDADE',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ_CPF_CONTRATADO',
                            consolidacao.CONTRATADO_DESCRICAO: 'CONTRATADO', consolidacao.DESPESA_DESCRICAO: 'OBJETO'}
        planilha_original = path.join(config.diretorio_dados, 'MG', 'portal_transparencia', 'Belo Horizonte',
                                      'contratacaocorona.xlsx')
        df_original = pd.read_excel(planilha_original)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_BeloHorizonte
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'MG', get_codigo_municipio_por_nome('Belo Horizonte', 'MG'), data_extracao,
                               self.pos_processar_contratacoes_capital)
        return df

    def pos_processar_contratacoes_capital(self, df):
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Belo Horizonte'
        df = df.rename(columns={'PROCESSO_COMPRA': 'NÚMERO DO PROCESSO DE COMPRA'})
        return df
