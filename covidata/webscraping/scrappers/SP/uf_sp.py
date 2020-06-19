import calendar
import datetime
import locale
import time
from datetime import datetime
from os import path

import requests
from bs4 import BeautifulSoup, Tag
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

import config
from webscraping.downloader import download
from webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_SP(SeleniumDownloader):
    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'SP', 'portal_transparencia'), url)

    def _executar(self):
        wait = WebDriverWait(self.driver, 30)

        frame = wait.until(EC.visibility_of_element_located((By.TAG_NAME, 'iframe')))
        src = frame.get_attribute("src")

        self.driver.get(src)

        element = wait.until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, '[CSV]')))
        self.driver.execute_script("arguments[0].click();", element)


def __portal_transparencia_estadual():
    print('Portal de transparência estadual...')
    start_time = time.time()
    pt_SP = PortalTransparencia_SP(config.url_pt_SP)
    pt_SP.download()
    print("--- %s segundos ---" % (time.time() - start_time))


def __portal_transparancia_capital():
    print('Portal de transparência municipal...')
    start_time = time.time()

    # TODO: Testar no Linux e no Mac
    locale.setlocale(locale.LC_TIME, "pt-BR")
    mes_inicial = 3
    mes_atual = datetime.now().month
    meses = []

    if mes_atual > mes_inicial:
        meses = [__get_nome_mes(i) for i in range(mes_inicial, mes_atual + 1)]
    elif mes_atual <= mes_inicial:
        # de um ano para o outro
        meses = [__get_nome_mes(i) for i in range(mes_inicial, 13)]
        meses = meses + [__get_nome_mes(i) for i in range(1, mes_atual + 1)]

    __baixar_arquivos(meses)

    print("--- %s segundos ---" % (time.time() - start_time))


def __baixar_arquivos(meses):
    url = config.url_pt_SaoPaulo
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    tags = soup.find_all('strong')

    for tag in tags:
        for mes in meses:
            if mes in tag.text:
                __baixar_arquivo_mensal(mes, tag)


def __baixar_arquivo_mensal(mes, tag):
    links = tag.find_all('a', string='Excel')
    if len(links) == 0:
        for sibling in tag.next_siblings:
            if isinstance(sibling, Tag):
                links = sibling.find_all('a', string='Excel')
                if len(links) > 0:
                    break
    if len(links) > 0:
        link = links[0]
        url = link.attrs['href']
        diretorio = path.join(config.diretorio_dados, 'SP', 'portal_transparencia', 'São Paulo')
        download(url, diretorio, path.join(diretorio, mes + '.xls'))


def __get_nome_mes(mes):
    nome_mes_atual = calendar.month_name[mes]
    nome_mes_atual = nome_mes_atual[0].upper() + nome_mes_atual[1:]
    return nome_mes_atual


def main():
    __portal_transparencia_estadual()
    __portal_transparancia_capital()


main()
