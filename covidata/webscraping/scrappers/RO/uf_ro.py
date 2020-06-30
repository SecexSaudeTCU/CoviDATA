import logging
import time
from os import path

from covidata import config
from covidata.webscraping.downloader import FileDownloader


def main():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_RO = FileDownloader(path.join(config.diretorio_dados, 'RO', 'portal_transparencia'), config.url_pt_RO,
                           'Despesas.CSV')
    pt_RO.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_PortoVelho_1 = FileDownloader(path.join(config.diretorio_dados, 'RO', 'portal_transparencia', 'Porto Velho'),
                                     config.url_pt_PortoVelho_despesas_por_credor, 'Despesas_por_credor.CSV')
    pt_PortoVelho_1.download()

    pt_PortoVelho_2 = FileDownloader(path.join(config.diretorio_dados, 'RO', 'portal_transparencia', 'Porto Velho'),
                                     config.url_pt_PortoVelho_despesas_por_instituicao, 'Despesas_por_instituicao.CSV')
    pt_PortoVelho_2.download()

    pt_PortoVelho_3 = FileDownloader(path.join(config.diretorio_dados, 'RO', 'portal_transparencia', 'Porto Velho'),
                                     config.url_pt_PortoVelho_tipos_despesas, 'Tipos_despesas.CSV')
    pt_PortoVelho_3.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))
