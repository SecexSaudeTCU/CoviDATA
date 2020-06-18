from webscraping.downloader import FileDownloader
from os import path
from webscraping.selenium.downloader import SeleniumDownloader
import time
import config

#TODO: Falta o scraping do TC - por enquanto, alto custo para pouca informação.

class PortalTransparencia_Fortaleza(SeleniumDownloader):

    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'CE', 'portal_transparencia', 'Fortaleza'), url)

    def download(self):
        button = self.driver.find_element_by_id('download')
        button.click()

        # Aguarda o download
        time.sleep(5)

        self.driver.close()
        self.driver.quit()


def main():
    pt_CE = FileDownloader(path.join(config.diretorio_dados, 'CE', 'portal_transparencia'), config.url_pt_CE,
                           'gasto_covid_dados_abertos.xlsx')
    pt_CE.download()

    pt_Fortaleza = PortalTransparencia_Fortaleza(config.url_pt_Fortaleza)
    pt_Fortaleza.download()
