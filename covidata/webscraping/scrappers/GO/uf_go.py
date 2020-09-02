import datetime
import logging
import time
from os import path

from covidata import config
from covidata.webscraping.scrappers.GO.PT_Goiania import PT_Despesas_Goiania
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.GO.consolidacao_GO import consolidar


def main(df_consolidado):
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')

    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_GO = FileDownloader(path.join(config.diretorio_dados, 'GO', 'portal_transparencia'), config.url_pt_GO,
                           'aquisicoes.csv')
    pt_GO.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao, df_consolidado)
    logger.info("--- %s segundos ---" % (time.time() - start_time))

