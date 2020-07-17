import locale
import logging
import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import UnexpectedAlertPresentException
from webdriver_manager.chrome import ChromeDriverManager

from covidata import config
from covidata.persistencia.dao import persistir
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

def main():
    logger = logging.getLogger('covidata')
    logger.info('Tribunal de Contas municipal...')
    start_time = time.time()

    url = config.url_tcm_SP
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'lxml')

    numero_paginas, pagina_atual = __get_paginacao(soup)
    colunas, lista_linhas = __extrair_tabela(soup)

    driver = configurar_browser()
    driver.get(url)

    for i in range(pagina_atual + 1, numero_paginas + 1):
        linhas = __processar_pagina(driver, i, lista_linhas)
        lista_linhas += linhas

    driver.close()
    driver.quit()

    df = pd.DataFrame(lista_linhas, columns=colunas)
    # persistir(df, 'tcm', 'licitacoes', 'SP')
    persistir(df, 'tcm', 'licitacoes', 'SP')

    logger.info("--- %s segundos ---" % (time.time() - start_time))


def __processar_pagina(driver, i, lista_linhas):
    logger = logging.getLogger("covidata")
    logger.info(f'Processando página {i}...')

    #link = driver.find_element_by_link_text(str(i))
    #link.click()

    wait = WebDriverWait(driver, 30)
    link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, str(i))))
    driver.execute_script("arguments[0].click();", link)

    # Aguarda o completo carregamento da tela
    time.sleep(20)

    try:
        soup = BeautifulSoup(driver.page_source, 'lxml')
    except UnexpectedAlertPresentException:
        # Tenta novamente
        #link.click()
        driver.execute_script("arguments[0].click();", link)

        # Aguarda o completo carregamento da tela
        time.sleep(20)

        soup = BeautifulSoup(driver.page_source, 'lxml')
    _, linhas = __extrair_tabela(soup)
    linhas = __checar_linhas_repetidas(driver, linhas, link, lista_linhas)
    return linhas


def __checar_linhas_repetidas(driver, linhas, link, lista_linhas):
    novos_ids = [linha[0] for linha in linhas]
    ids = [lista_linhas[0] for linha in lista_linhas]
    if novos_ids[0] == ids[:-15]:
        # Registros repetidos - recarregamento da página não funcionou, é necessário carregar novamente.
        logger = logging.getLogger('covidata')
        logger.info('Registros repetidos - nova tentativa de carregamento será feita...')
        link.click()

        # Aguarda o completo carregamento da tela
        time.sleep(20)

        soup = BeautifulSoup(driver.page_source, 'lxml')
        _, linhas = __extrair_tabela(soup)
    return linhas


def __get_paginacao(soup):
    paginador = soup.find(id='gridLicitacao_DXPagerBottom')
    texto_paginacao = paginador.contents[1].get_text()
    x = re.findall("Page ([0-9]+) of ([0-9]+)", texto_paginacao)
    pagina_atual = int(x[0][0])
    numero_paginas = int(x[0][1])
    return numero_paginas, pagina_atual


def __extrair_tabela(soup):
    tabela = soup.find(id='gridLicitacao_DXMainTable')
    linhas = tabela.find_all('tr', recursive=False)

    if len(linhas) == 0:
        linhas = tabela.find('tbody').find_all('tr', recursive=False)

    titulos = linhas[0]

    titulos_colunas = titulos.find_all('td')

    indices = range(4, len(titulos_colunas), 3)
    colunas = [titulos_colunas[i].get_text() for i in indices]
    linhas = linhas[1:]
    lista_linhas = []

    for linha in linhas:
        data = linha.find_all("td")
        nova_linha = [data[i].get_text() for i in range(1, len(colunas) + 1)]
        lista_linhas.append(nova_linha)

    return colunas, lista_linhas


def configurar_browser():
    chromeOptions = webdriver.ChromeOptions()
    # chromeOptions.add_argument('--headless')
    locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")
    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chromeOptions)
    return driver

