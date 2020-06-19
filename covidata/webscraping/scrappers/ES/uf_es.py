from os import path

import config
from webscraping.downloader import FileDownloader


def main():
    pt_ES = FileDownloader(path.join(config.diretorio_dados, 'ES', 'portal_transparencia'), config.url_pt_ES,
                           'dados-contratos-emergenciais-covid-19.csv')
    pt_ES.download()

    pt_Vitoria = FileDownloader(path.join(config.diretorio_dados, 'ES', 'portal_transparencia', 'Vitoria'),
                               config.url_pt_Vitoria, 'TransparenciaWeb.Licitacoes.Lista.xlsx')
    pt_Vitoria.download()
