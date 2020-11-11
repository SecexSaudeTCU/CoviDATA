from os import path

import logging
import os
import pandas as pd
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.json.parser import JSONParser
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
        arquivos = os.listdir(diretorio)

        if len(arquivos) > 0:
            arquivo = arquivos[0]
            os.rename(path.join(diretorio, arquivo), path.join(diretorio, 'ComprasEmergenciaisMS_COVID19.csv'))

    def consolidar(self, data_extracao):
        return self.__consolidar_compras_emergenciais(data_extracao), False

    def __consolidar_compras_emergenciais(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATADO_CNPJ: 'CPF/CNPJ', consolidacao.CONTRATADO_DESCRICAO: 'Credor',
                            consolidacao.CONTRATANTE_DESCRICAO: 'Órgão', consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.VALOR_CONTRATO: 'Valor Total da Compra'}
        planilha_original = path.join(config.diretorio_dados, 'MS', 'portal_transparencia',
                                      'ComprasEmergenciaisMS_COVID19.csv')
        df_original = pd.read_csv(planilha_original, sep=';', header=3, index_col=False)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_MS
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               fonte_dados, 'MS', '', data_extracao)
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


class PT_CampoGrande_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()

        PortalTransparenciaCampoGrande().parse()

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        despesas_capital = self.__consolidar_despesas_capital(data_extracao)
        return despesas_capital, False

    def __consolidar_despesas_capital(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'orgao',
                            consolidacao.CONTRATADO_DESCRICAO: 'nomefornecedor',
                            consolidacao.CONTRATADO_CNPJ: 'fornecedor',
                            consolidacao.DESPESA_DESCRICAO: 'itemclassificacaodespesa'}
        planilha_original = path.join(config.diretorio_dados, 'MS', 'portal_transparencia', 'Campo Grande',
                                      'despesas.xlsx')
        df_original = pd.read_excel(planilha_original, header=1)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_CampoGrande
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'MS', get_codigo_municipio_por_nome('Campo Grande', 'MS'), data_extracao,
                               self.pos_processar_despesas_capital)
        return df

    def pos_processar_despesas_capital(self, df):
        df = df.rename(columns={'PROCESSO DE ORIGEM': 'NÚMERO PROCESSO'})
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Campo Grande'
        return df


class PortalTransparenciaCampoGrande(JSONParser):
    def __init__(self):
        super().__init__(config.url_pt_CampoGrande, 'num', 'despesas', 'portal_transparencia', 'MS', 'Campo Grande')

    def _get_elemento_raiz(self, conteudo):
        return conteudo['data']
