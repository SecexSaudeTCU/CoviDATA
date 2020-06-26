import re
import time

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import UnexpectedAlertPresentException
from webdriver_manager.chrome import ChromeDriverManager

from covidata import config
import pandas as pd
from covidata.persistencia.dao import persistir2
import locale
import logging

import re
from urllib.parse import urlparse


def main():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência da capital...')
    start_time = time.time()

    url = config.url_pt_PortoAlegre
    parsed_uri = urlparse(url)
    url_base = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)

    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'lxml')

    proximas_paginas = __get_paginacao(soup)

    nomes_colunas = ['Código da licitação', 'Data/hora de registro', 'Objeto', 'Tipo', 'Status']

    registros = soup.findAll('div', {'class': 'item-registro xs-col-12'})
    for registro in registros:
        textos = re.split('\r\n', registro.findAll('h2')[0].get_text().strip())
        codigo_licitacao = textos[0]
        data_hora_registro = textos[2]

        conteudo = registro.findAll('div')[0]
        divs = conteudo.findAll('div')
        div_descricao_registro = divs[0]
        objeto = div_descricao_registro.findAll('p')[0].get_text()
        div_detalhes = divs[1]
        spans = div_detalhes.findAll('span')
        tipo = spans[0].get_text()
        status = spans[1].get_text()

        dados_adicionais = spans[2]
        url_detalhes = dados_adicionais.findAll('a', {'title':'Dados do Processo'})[0].attrs['href']
        url_detalhes = url_base + url_detalhes

        driver = __get_browser(url_detalhes)
        html = driver.page_source

        soup_detalhes = BeautifulSoup(html, 'lxml')
        dados_licitacao = soup_detalhes.findAll('p', {'class':'ff1 f400 fs16 fcW'})[0]
        bs = dados_licitacao.findAll('b')
        aplicar_decreto = bs[1].next_sibling.string.strip()
        inicio_propostas = bs[3].next_sibling.string.strip()
        print('')

    print("--- %s segundos ---" % (time.time() - start_time))

def __get_browser(url):
    chromeOptions = webdriver.ChromeOptions()
    chromeOptions.add_argument('--headless')

    locale.setlocale(locale.LC_ALL, "pt_br")

    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chromeOptions)
    driver.get(url)
    return driver

def __get_paginacao(soup):
    paginador = soup.findAll("ul", {"class": "pagination"})[0]
    paginas = paginador.findAll('li')
    proximas_paginas = []

    for pagina in paginas[1:]:
        link = pagina.findAll('a')[0].attrs['href']
        proximas_paginas.append(link)

    return proximas_paginas


#main()
