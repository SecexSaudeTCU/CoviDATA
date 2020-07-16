import datetime
import logging
import os
import time
from os import path

import requests
from bs4 import BeautifulSoup

from covidata import config
from covidata.util.excel import exportar_arquivo_para_xlsx
from covidata.webscraping.downloader import download
from covidata.webscraping.scrappers.MT.consolidacao_MT import consolidar


def pt_Cuiaba():
    page = requests.get(config.url_pt_Cuiaba)
    soup = BeautifulSoup(page.content, 'html5lib')

    baixar_arquivo(soup, descricao='PLANILHA INFORMATIVA SOBRE OS GASTOS EMERGENCIAIS - SMASDH',
                   nome_arquivo='planilha_gastos_emergenciais_SMASDH.pdf')
    baixar_arquivo(soup, descricao='PLANILHA INFORMATIVA SOBRE OS GASTOS EMERGENCIAIS - SMS ',
                   nome_arquivo='planilha_gastos_emergenciais_SMS.pdf')
    baixar_arquivo(soup, descricao='PLANILHA INFORMATIVA SOBRE OS GASTOS EMERGENCIAIS - SMG - SME',
                   nome_arquivo='planilha_gastos_emergenciais_SMSU_SME_SMGE.pdf')
    baixar_arquivo(soup, descricao='RECURSOS RECEBIDOS E APLICADOS ', nome_arquivo='recursos_recebidos_aplicados.pdf')
    baixar_arquivo(soup, descricao='RELAÇÃO POR ITEM ', nome_arquivo='descritivos_aquisicoes.pdf')


def baixar_arquivo(soup, descricao, nome_arquivo):
    span = soup.find_all('span', string=descricao)[0]
    link = span.parent.find_all('a')[0].attrs['href']
    url = 'http://covid.cuiaba.mt.gov.br/' + link
    diretorio = os.path.join(config.diretorio_dados, 'MT', 'portal_transparencia', 'Cuiabá')
    download(url, diretorio, os.path.join(diretorio, nome_arquivo))


def pt_MT():
    diretorio_dados = path.join(config.diretorio_dados, 'MT', 'portal_transparencia')

    if not path.exists(diretorio_dados):
        os.makedirs(diretorio_dados)

    # Salva os cookis que serão necessários posteriormente para o download
    s = requests.Session()
    r = s.get('http://consultas.transparencia.mt.gov.br/compras/contratos_covid/')
    r = s.get(config.url_pt_MT)

    with open(os.path.join(diretorio_dados, 'transparencia_excel.xls'), 'wb') as f:
        f.write(r.content)

    exportar_arquivo_para_xlsx(path.join(config.diretorio_dados, 'MT', 'portal_transparencia'),
                               'transparencia_excel.xls', 'transparencia_excel.xlsx')


def main():
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_MT()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_Cuiaba()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao)
    logger.info("--- %s segundos ---" % (time.time() - start_time))

