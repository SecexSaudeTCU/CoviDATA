from os import path

import logging
import pandas as pd
import time

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_ES_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()
        pt_ES = FileDownloader(path.join(config.diretorio_dados, 'ES', 'portal_transparencia'), config.url_pt_ES,
                               'dados-contratos-emergenciais-covid-19.csv')
        pt_ES.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_dados_contratos_emergenciais(data_extracao), False

    def __consolidar_dados_contratos_emergenciais(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão Contratante',
                            consolidacao.CONTRATADO_DESCRICAO: 'Nome do Contratado',
                            consolidacao.CONTRATADO_CNPJ: 'CPF / CNPJ do Contratado',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto'}
        planilha_original = path.join(config.diretorio_dados, 'ES', 'portal_transparencia',
                                      'dados-contratos-emergenciais-covid-19.csv')
        df_original = pd.read_csv(planilha_original, encoding="ISO-8859-1", sep=';')
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_ES
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               fonte_dados, 'ES', '', data_extracao)
        return df


class PT_Vitoria_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_Vitoria = FileDownloader(path.join(config.diretorio_dados, 'ES', 'portal_transparencia', 'Vitoria'),
                                    config.url_pt_Vitoria, 'TransparenciaWeb.Licitacoes.Lista.xlsx')
        pt_Vitoria.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_licitacoes_capital(data_extracao), False

    def __consolidar_licitacoes_capital(self, data_extracao):
        dicionario_dados = {consolidacao.DESPESA_DESCRICAO: 'Objeto'}
        planilha_original = path.join(config.diretorio_dados, 'ES', 'portal_transparencia', 'Vitoria',
                                      'TransparenciaWeb.Licitacoes.Lista.xlsx')
        df_original = pd.read_excel(planilha_original, header=3)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Vitoria
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'ES', get_codigo_municipio_por_nome('Vitória', 'ES'), data_extracao)
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Vitória'
        return df
