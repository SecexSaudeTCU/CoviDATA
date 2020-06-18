import time
from os import path

import config
from webscraping.downloader import FileDownloader
from webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_Macapa(SeleniumDownloader):
    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'AP', 'portal_transparencia', 'Macapá'), url)

    def download(self):
        button = self.driver.find_element_by_class_name('buttons-excel')
        button.click()

        # Aguarda o download
        time.sleep(5)

        self.driver.close()
        self.driver.quit()


def main():
    pt_AP = FileDownloader(path.join(config.diretorio_dados, 'AP', 'portal_transparencia'), config.url_pt_AP, 'AP.json')
    pt_AP.download()

    pt_Macapa = PortalTransparencia_Macapa(config.url_pt_Macapa)
    pt_Macapa.download()
