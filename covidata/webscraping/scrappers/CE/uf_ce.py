from os import path

import datetime
import logging
import time

from covidata import config
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.CE.consolidacao_CE import consolidar


def main(df_consolidado):
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_CE = FileDownloader(path.join(config.diretorio_dados, 'CE', 'portal_transparencia'), config.url_pt_CE,
                           'gasto_covid_dados_abertos.xlsx')
    pt_CE.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))


    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao, df_consolidado)
    logger.info("--- %s segundos ---" % (time.time() - start_time))
