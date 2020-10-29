import requests
import time

import logging
import pandas as pd
from bs4 import BeautifulSoup

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.persistencia.dao import persistir
from covidata.webscraping.scrappers.scrapper import Scraper
from os import path

class PT_BA_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()
        page = requests.get(config.url_pt_BA)
        soup = BeautifulSoup(page.content, 'html5lib')
        tabela = soup.find_all('table')[0]
        tbody = tabela.find_all('tbody')[0]
        titulos = tbody.contents[1]
        titulos = titulos.find_all('strong')
        nomes_colunas = ['Link para o contrato']
        nomes_colunas += [titulo.get_text() for titulo in titulos]
        linhas = []

        trs = tbody.find_all('tr')
        for i in range(2, len(trs)):
            tds = trs[i].find_all('td')
            links = tds[1].find_all('a')
            if len(links) > 0:
                link_contrato = links[0].attrs['href']
                linha = [link_contrato] + [td.get_text() for td in tds]
                linhas.append(linha)

        df = pd.DataFrame(linhas, columns=nomes_colunas)
        persistir(df, 'portal_transparencia', 'contratos', 'BA')
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_contratos(data_extracao), False

    def consolidar_contratos(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATADO_DESCRICAO: 'CONTRATADO', consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                            consolidacao.DESPESA_DESCRICAO: 'OBJETO', consolidacao.VALOR_CONTRATO: 'VALOR'}
        planilha_original = path.join(config.diretorio_dados, 'BA', 'portal_transparencia', 'contratos.xls')
        df_original = pd.read_excel(planilha_original, header=4)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_BA
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL, fonte_dados, 'BA', '',
                               data_extracao, None)
        return df