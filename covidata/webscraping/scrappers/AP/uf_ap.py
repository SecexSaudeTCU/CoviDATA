import time
from os import path

import config
from webscraping.downloader import FileDownloader
from webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_Macapa(SeleniumDownloader):
    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'AP', 'portal_transparencia', 'Macapá'), url)

    def _executar(self):
        button = self.driver.find_element_by_class_name('buttons-excel')
        button.click()


def main():
    print('Portal de transparência estadual...')
    start_time = time.time()
    pt_AP = FileDownloader(path.join(config.diretorio_dados, 'AP', 'portal_transparencia'), config.url_pt_AP, 'AP.json')
    pt_AP.download()
    print("--- %s segundos ---" % (time.time() - start_time))

    print('Portal de transparência da capital...')
    start_time = time.time()
    pt_Macapa = PortalTransparencia_Macapa(config.url_pt_Macapa)
    pt_Macapa.download()
    print("--- %s segundos ---" % (time.time() - start_time))
