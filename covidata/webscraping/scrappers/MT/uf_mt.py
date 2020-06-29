import logging
import os
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

from covidata import config
from covidata.persistencia.dao import persistir2
from covidata.webscraping.downloader import download


def pt_MT():
    page = requests.get(config.url_pt_MT)
    soup = BeautifulSoup(page.content, 'html5lib')
    tabela = soup.find_all('table')[0]
    nomes_colunas = ['Link para o contrato'] + [th.get_text() for th in tabela.find_all('th')]
    linhas = tabela.find_all('tr')[1:]

    linhas_df = []

    for linha in linhas:
        tds = linha.find_all('td')
        link_contrato = config.url_pt_MT + '/' + tds[1].find_all('a')[0].attrs['href']
        colunas = [link_contrato] + [td.get_text() for td in tds]
        linhas_df.append(colunas)

    df = pd.DataFrame(linhas_df, columns=nomes_colunas)
    persistir2(df, 'portal_transparencia', 'Contratos', 'MT')


def pt_Cuiaba():
    page = requests.get(config.url_pt_Cuiaba)
    soup = BeautifulSoup(page.content, 'html5lib')

    baixar_arquivo(soup, descricao='PLANILHA INFORMATIVA SOBRE OS GASTOS EMERGENCIAIS - SMASDH',
                   nome_arquivo='planilha_gastos_emergenciais_SMASDH.pdf')
    baixar_arquivo(soup, descricao='PLANILHA INFORMATIVA SOBRE OS GASTOS EMERGENCIAIS - SMS ',
                   nome_arquivo='planilha_gastos_emergenciais_SMS.pdf')
    baixar_arquivo(soup, descricao='PLANILHA INFORMATIVA SOBRE OS GASTOS EMERGENCIAIS - SMSU- SME - SMGE',
                   nome_arquivo='planilha_gastos_emergenciais_SMSU_SME_SMGE.pdf')
    baixar_arquivo(soup, descricao='RECURSOS RECEBIDOS E APLICADOS ', nome_arquivo='recursos_recebidos_aplicados.pdf')
    baixar_arquivo(soup, descricao='RELAÇÃO POR ITEM ', nome_arquivo='descritivos_aquisicoes.pdf')


def baixar_arquivo(soup, descricao, nome_arquivo):
    span = soup.find_all('span', string=descricao)[0]
    link = span.parent.find_all('a')[0].attrs['href']
    url = 'http://covid.cuiaba.mt.gov.br/' + link
    diretorio = os.path.join(config.diretorio_dados, 'MT', 'portal_transparencia', 'Cuiabá')
    download(url, diretorio, os.path.join(diretorio, nome_arquivo))


def main():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_MT()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_Cuiaba()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

