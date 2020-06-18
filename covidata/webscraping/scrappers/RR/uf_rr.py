from os import path

import config
from webscraping.downloader import FileDownloader


# TODO: Falta TCE RR

def main():
    pt_RR = FileDownloader(path.join(config.diretorio_dados, 'RR', 'portal_transparencia'), config.url_pt_RR,
                           'covid19_1592513934.xls')
    pt_RR.download()

    pt_BoaVista = FileDownloader(path.join(config.diretorio_dados, 'RR', 'portal_transparencia', 'Boa Vista'),
                                 config.url_pt_BoaVista, 'contrato_covid-19.txt')
    pt_BoaVista.download()
