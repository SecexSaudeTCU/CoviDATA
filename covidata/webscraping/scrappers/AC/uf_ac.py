import datetime
import logging
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

from covidata import config
from covidata.persistencia.dao import persistir
from covidata.webscraping.scrappers.AC.consolidacao_AC import consolidar


def __extrair(url, informacao, indice):
    colunas, linhas_df = __extrair_tabela(url, indice)
    df = pd.DataFrame(linhas_df, columns=colunas)
    persistir(df, 'tce', informacao, 'AC')


def __extrair_tabela(url, indice):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    tabela = soup.find_all('table')[indice]
    linhas = tabela.find_all('tr')
    titulos = linhas[0]
    titulos_colunas = titulos.find_all('td')
    colunas = [titulo_coluna.get_text() for titulo_coluna in titulos_colunas]
    linhas = linhas[1:]
    lista_linhas = []

    for linha in linhas:
        data = linha.find_all("td")
        nova_linha = [data[i].get_text() for i in range(len(colunas))]
        lista_linhas.append(nova_linha)

    return colunas, lista_linhas


def tce_ac():
    start_time = time.time()
    __extrair(url=config.url_tce_AC_contratos, informacao='contratos', indice=0)
    __extrair(url=config.url_tce_AC_despesas, informacao='despesas', indice=0)
    __extrair(url=config.url_tce_AC_despesas, informacao='dispensas', indice=1)
    __extrair(url=config.url_tce_AC_contratos_municipios, informacao='contratos_municipios', indice=0)
    __extrair(url=config.url_tce_AC_despesas_municipios, informacao='despesas_municipios', indice=0)
    __extrair(url=config.url_tce_AC_despesas_municipios, informacao='dispensas_municipios', indice=1)
    return start_time


def main(df_consolidado):
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')
    logger.info('Tribunal de Contas estadual...')
    start_time = tce_ac()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao, df_consolidado)
    logger.info("--- %s segundos ---" % (time.time() - start_time))

# main()
