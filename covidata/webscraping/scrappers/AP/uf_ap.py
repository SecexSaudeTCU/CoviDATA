import time
from os import path

import config
from webscraping.json.parser import JSONParser
from webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_AP(JSONParser):

    def __init__(self):
        super().__init__(config.url_pt_AP, 'id', 'contratacoes', 'portal_transparencia', 'AP')

    def json_parse(self, conteudo):
        # Neste caso, não há elemento-raiz nomeado.
        return conteudo


class PortalTransparencia_Macapa(SeleniumDownloader):
    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'AP', 'portal_transparencia', 'Macapa'), url)

    def _executar(self):
        button = self.driver.find_element_by_class_name('buttons-excel')
        button.click()


def main():
    print('Portal de transparência estadual...')
    start_time = time.time()
    pt_AP = PortalTransparencia_AP()
    pt_AP.parse()
    print("--- %s segundos ---" % (time.time() - start_time))

    print('Portal de transparência da capital...')
    start_time = time.time()
    pt_Macapa = PortalTransparencia_Macapa(config.url_pt_Macapa)
    pt_Macapa.download()
    print("--- %s segundos ---" % (time.time() - start_time))
