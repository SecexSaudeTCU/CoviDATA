import datetime
import logging
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4 import Tag

from covidata import config
from covidata.persistencia.dao import persistir


def pt_TO():
    linhas = []
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
            elif (not ('Total do Processo/Contrato' in texto)) and (not ('Total do Tipo de Contratação' in texto)) and (
                    not ('Total do Órgão' in texto)):
                trs = sibling.find_all('tr')
                for tr in trs:
                    tds = tr.find_all('td')
                    if len(tds) >= len(nomes_colunas):
                        # indicativo de linha de conteúdo
                        linha = [nome_orgao]

                        for td in tds:
                            texto_tag = td.get_text().strip()
                            if texto_tag != '':
                                linha.append(texto_tag)

                        # A quinta coluna, caso preenchida, tem que estar no formato data, e a sexta coluna,
                        # caso prenchida, tem que ser um número.  Estas verificações têm por objetivo contornar o
                        # problema das colunas não obrigatórias.
                        try:
                            datetime.datetime.strptime(linha[5], '%d/%m/%Y')
                        except ValueError:
                            linha.insert(5, '')

                        if not linha[6].isnumeric():
                            linha.insert(6, '')

                        linhas.append(linha)

    df = pd.DataFrame(linhas, columns=nomes_colunas)
    persistir(df, 'portal_transparencia', 'contratos', 'TO')


def main():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_TO()
    logger.info("--- %s segundos ---" % (time.time() - start_time))
