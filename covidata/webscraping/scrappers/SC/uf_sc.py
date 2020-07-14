from os import path
import logging
import time
from datetime import datetime

from covidata import config
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.SC.consolidacao_SC import consolidar


def main():
    data_extracao = datetime.now()
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()

    pt_SC = FileDownloader(path.join(config.diretorio_dados, 'SC', 'portal_transparencia'), config.url_pt_SC_contratos,
                           'contrato_item.xlsx')
    pt_SC.download()

    pt_SC = FileDownloader(path.join(config.diretorio_dados, 'SC', 'portal_transparencia'), config.url_pt_SC_despesas,
                           'analisedespesa.csv')
    pt_SC.download()

    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_Florianopolis = FileDownloader(path.join(config.diretorio_dados, 'SC', 'portal_transparencia', 'Florianopolis'),
                           config.url_pt_Florianopolis, 'aquisicoes.csv')
    pt_Florianopolis.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao)
    logger.info("--- %s segundos ---" % (time.time() - start_time))
