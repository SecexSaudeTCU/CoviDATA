from os import path

import datetime
import logging
import time

from covidata import config
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.MG.consolidacao_MG import consolidar


def main(df_consolidado):
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')

    logger.info('Portal de transparência da capital...')
    start_time = time.time()

    pt_BeloHorizonte = FileDownloader(path.join(config.diretorio_dados, 'MG', 'portal_transparencia', 'Belo Horizonte'),
                                      config.url_pt_BeloHorizonte, 'contratacaocorona.xlsx')
    pt_BeloHorizonte.download()

    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao, df_consolidado)
    logger.info("--- %s segundos ---" % (time.time() - start_time))
