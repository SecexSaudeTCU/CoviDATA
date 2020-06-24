import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4 import Tag
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

from covidata import config
from covidata.persistencia.dao import persistir_dados_hierarquicos
import logging


def pt_TO():
    linhas = []

    colunas_total_por_processo = ['Órgão', 'Processo', 'Total do Processo/Contrato']
    colunas_total_por_tipo_contratacao = ['Órgão', 'Tipo de Contratação', 'Total do Tipo de Contratação']
    colunas_total_por_orgao = ['Órgão', 'Total do Órgão']

    linhas_total_por_processo = []
    linhas_total_por_tipo_contratacao = []
    linhas_total_por_orgao = []

    page = requests.get(config.url_pt_TO)
    soup = BeautifulSoup(page.content, 'html.parser')
    linha_titulos = soup.find(id='tit_consulta_contrato_covid_2__SCCS__1')
    colunas = linha_titulos.find_all('td')
    nomes_colunas = ['Órgão']

    for coluna in colunas:
        nome_coluna = coluna.get_text().strip()

        if nome_coluna != '':
            nomes_colunas.append(nome_coluna)

    for sibling in linha_titulos.next_siblings:
        if isinstance(sibling, Tag):
            # se for o nome do órgão
            texto = sibling.get_text().strip()
            if texto.startswith('Órgão - '):
                nome_orgao = texto[len('Órgão - '): len(texto)]
            elif 'Total do Processo/Contrato' in texto:
                valor = texto[texto.rfind('\n') + 1: len(texto)]
                processo = linhas[-1][1]
                linhas_total_por_processo.append([nome_orgao, processo, valor])
            elif 'Total do Tipo de Contratação' in texto:
                valor = texto[texto.rfind('\n') + 1: len(texto)]
                tipo_contratacao = linhas[-1][2]
                linhas_total_por_tipo_contratacao.append([nome_orgao, tipo_contratacao, valor])
            elif 'Total do Órgão' in texto:
                valor = texto[texto.rfind('\n') + 1: len(texto)]
                linhas_total_por_orgao.append([nome_orgao, valor])
            else:
                tds = sibling.find_all('td')
                if len(tds) >= len(nomes_colunas):
                    # indicativo de linha de conteúdo
                    linha = [nome_orgao]

                    for td in tds:
                        texto_tag = td.get_text().strip()
                        if texto_tag != '':
                            linha.append(texto_tag)

                    linhas.append(linha)

    __persistir(colunas_total_por_orgao, colunas_total_por_processo, colunas_total_por_tipo_contratacao, linhas,
                linhas_total_por_orgao, linhas_total_por_processo, linhas_total_por_tipo_contratacao, nomes_colunas)


def __persistir(colunas_total_por_orgao, colunas_total_por_processo, colunas_total_por_tipo_contratacao, linhas,
                linhas_total_por_orgao, linhas_total_por_processo, linhas_total_por_tipo_contratacao, nomes_colunas):
    df = pd.DataFrame(linhas, columns=nomes_colunas)
    df_total_por_processo = pd.DataFrame(linhas_total_por_processo, columns=colunas_total_por_processo)
    df_total_por_tipo_contratacao = pd.DataFrame(linhas_total_por_tipo_contratacao,
                                                 columns=colunas_total_por_tipo_contratacao)
    df_total_por_orgao = pd.DataFrame(linhas_total_por_orgao, columns=colunas_total_por_orgao)
    persistir_dados_hierarquicos(df, {"Totais por processo": df_total_por_processo,
                                      "Totais por tipo de contratação": df_total_por_tipo_contratacao,
                                      "Totais por órgão": df_total_por_orgao},
                                 'portal_transparencia', 'contratos', 'TO')


def __get_browser(url):
    chromeOptions = webdriver.ChromeOptions()
    chromeOptions.add_argument('--headless')
    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chromeOptions)
    driver.get(url)
    return driver


def main():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_TO()
    logger.info("--- %s segundos ---" % (time.time() - start_time))
