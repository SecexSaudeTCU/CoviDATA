import time
from os import path

import config
from webscraping.downloader import FileDownloader
from webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_Maceio(SeleniumDownloader):
    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'AM', 'portal_transparencia', 'Manaus'), url)

    def _executar(self):
        #TODO
        pass

def main():
    print('Portal de transparência estadual...')
    start_time = time.time()
    pt_AL = FileDownloader(path.join(config.diretorio_dados, 'AL', 'portal_transparencia'), config.url_pt_AL,
                           'DESPESAS COM COVID-19.xls')
    pt_AL.download()
    print("--- %s segundos ---" % (time.time() - start_time))

main()