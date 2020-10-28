import os
import pandas as pd
import time

import logging

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from os import path

from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_Macapa_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')

        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_Macapa = PortalTransparencia_Macapa()
        pt_Macapa.download()

        # Renomeia o arquivo
        diretorio = path.join(config.diretorio_dados, 'AP', 'portal_transparencia', 'Macapa')
        arquivo = os.listdir(diretorio)[0]
        os.rename(path.join(diretorio, arquivo), path.join(diretorio, 'transparencia.xlsx'))

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        logger = logging.getLogger('covidata')
        logger.info('Iniciando consolidação dados Amapá')
        return self.consolidar_contratacoes_capital(data_extracao), False

    def consolidar_contratacoes_capital(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATADO_CNPJ: 'Contratado - CNPJ / CPF',
                            consolidacao.DESPESA_DESCRICAO: 'Descrição de bem ou serviço',
                            consolidacao.CONTRATANTE_DESCRICAO: 'Órgão Contratante',
                            consolidacao.VALOR_CONTRATO: 'Valor contratado'}
        planilha_original = path.join(config.diretorio_dados, 'AP', 'portal_transparencia', 'Macapa',
                                      'transparencia.xlsx')
        df_original = pd.read_excel(planilha_original, header=1)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Macapa
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'AP', get_codigo_municipio_por_nome('Macapá', 'AP'), data_extracao,
                               self.pos_processar_contratacoes_capital)
        return df

    def pos_processar_contratacoes_capital(self, df):
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Macapá'

        df['temp'] = df[consolidacao.CONTRATANTE_DESCRICAO]
        df[consolidacao.CONTRATANTE_DESCRICAO] = df.apply(lambda row: row['temp'][0:row['temp'].find('-')], axis=1)

        df = df.drop(['temp'], axis=1)

        df['temp'] = df[consolidacao.CONTRATADO_CNPJ]
        df[consolidacao.CONTRATADO_DESCRICAO] = df.apply(lambda row: row['temp'][0:row['temp'].find('/')], axis=1)
        df[consolidacao.CONTRATADO_CNPJ] = df.apply(lambda row: row['temp'][row['temp'].find('/') + 1:len(row['temp'])],
                                                    axis=1)
        df = df.drop(['temp'], axis=1)

        return df


class PortalTransparencia_Macapa(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'AP', 'portal_transparencia', 'Macapa'),
                         config.url_pt_Macapa)

    def _executar(self):
        button = self.driver.find_element_by_class_name('buttons-excel')
        button.click()
