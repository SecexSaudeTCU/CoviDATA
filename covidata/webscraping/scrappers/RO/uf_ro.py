import datetime
import logging
import time
from os import path

from covidata import config
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.RO.consolidacao_RO import consolidar


def main():
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt = FileDownloader(path.join(config.diretorio_dados, 'RO', 'portal_transparencia'), config.url_pt_RO,
                        'Despesas.CSV')
    pt.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao)
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    return logger
