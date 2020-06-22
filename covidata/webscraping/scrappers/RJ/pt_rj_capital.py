from os import path

import config
from webscraping.downloader import FileDownloader
import time

def main():
    print('Portal de transparência da capital...')
    start_time = time.time()
    pt_Rio_favorecidos = FileDownloader(
        path.join(config.diretorio_dados, 'RJ', 'portal_transparencia', 'Rio de Janeiro'),
        config.url_pt_Rio_favorecidos, 'Open_Data_Favorecidos_Covid19_2020.xlsx')
    pt_Rio_favorecidos.download()

    pt_Rio_contratos = FileDownloader(
        path.join(config.diretorio_dados, 'RJ', 'portal_transparencia', 'Rio de Janeiro'),
        config.url_pt_Rio_contratos, 'Open_Data_Contratos_Covid19_2020.xlsx')
    pt_Rio_contratos.download()

    pt_Rio_despesas_por_ato = FileDownloader(
        path.join(config.diretorio_dados, 'RJ', 'portal_transparencia', 'Rio de Janeiro'),
        config.url_pt_Rio_despesas_por_ato, '_arquivos_Open_Data_Desp_Ato_Covid19_2020.txt')
    pt_Rio_despesas_por_ato.download()
    print("--- %s segundos ---" % (time.time() - start_time))