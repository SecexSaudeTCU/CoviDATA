from os import path

import config
from webscraping.downloader import FileDownloader
import time

def main():
    print('Portal de transparência estadual...')
    start_time = time.time()
    pt_ES = FileDownloader(path.join(config.diretorio_dados, 'ES', 'portal_transparencia'), config.url_pt_ES,
                           'dados-contratos-emergenciais-covid-19.csv')
    pt_ES.download()
    print("--- %s segundos ---" % (time.time() - start_time))

    print('Portal de transparência da capital...')
    start_time = time.time()
    pt_Vitoria = FileDownloader(path.join(config.diretorio_dados, 'ES', 'portal_transparencia', 'Vitoria'),
                               config.url_pt_Vitoria, 'TransparenciaWeb.Licitacoes.Lista.xlsx')
    pt_Vitoria.download()
    print("--- %s segundos ---" % (time.time() - start_time))
