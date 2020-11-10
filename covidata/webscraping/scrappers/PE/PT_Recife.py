import logging
import os
import time
from glob import glob
from os import path
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pandas.errors import ParserError

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import salvar, consolidar_layout
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_Recife_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()

        page = requests.get(self.url)
        soup = BeautifulSoup(page.content, 'html.parser')
        div = soup.findAll("div", {"class": "module-content"})[0]
        itens = div.find_all('ul')[0].find_all('li')

        paginas_csv = self.__get_paginas(itens)

        for pagina in paginas_csv:
            parsed_uri = urlparse(self.url)
            result = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
            page = requests.get(result + pagina)
            soup = BeautifulSoup(page.content, 'html.parser')
            ul = soup.findAll('ul', {'class': 'dropdown-menu'})[0]
            link = ul.find_all('li')[1].find_all('a')[0].attrs['href']
            diretorio = path.join(config.diretorio_dados, 'PE', 'portal_transparencia', 'Recife')
            FileDownloader(diretorio, link,
                           link[link.rindex('/') + 1:len(link)]).download()

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def __get_paginas(self, itens):
        paginas_csv = set()
        for item in itens:
            item_csv = item.findAll('a', {'data-format': 'csv'})
            if len(item_csv) > 0:
                link = item_csv[0].attrs['href']
                paginas_csv.add(link)
        return paginas_csv

    def consolidar(self, data_extracao):
        logger = logging.getLogger('covidata')
        logger.info('Iniciando consolidação dados Pernambuco')
        dispensas = self.__consolidar_dispensas(data_extracao)
        return dispensas, False

    def __consolidar_dispensas(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'unidade',
                            consolidacao.DESPESA_DESCRICAO: 'objeto', consolidacao.CONTRATADO_CNPJ: 'cnpj_cpf',
                            consolidacao.CONTRATADO_DESCRICAO: 'nome_fornecedor',
                            consolidacao.VALOR_CONTRATO: 'valor_fornecedor'}
        df_final = pd.DataFrame()

        # Processando arquivos Excel
        planilhas = [y for x in os.walk(path.join(config.diretorio_dados, 'PE', 'portal_transparencia', 'Recife')) for y
                     in
                     glob(os.path.join(x[0], '*.csv'))]

        for planilha_original in planilhas:
            try:
                df_original = pd.read_csv(planilha_original, sep=';')
            except ParserError:
                df_original = pd.read_csv(planilha_original, header=1, sep=';')

            df = self.__processar_df_original(data_extracao, df_original, dicionario_dados)
            df_final = df_final.append(df)

        return df_final

    def __processar_df_original(self, data_extracao, df_original, dicionario_dados):
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Recife
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'PE', get_codigo_municipio_por_nome('Recife', 'PE'), data_extracao,
                               self.pos_processar_consolidar_dispensas)
        return df

    def pos_processar_consolidar_dispensas(self, df):
        df = df.astype({consolidacao.CONTRATADO_CNPJ: str})
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Recife'

        # Unifica colunas com nomes parecidos
        if 'data_de_empenho_contrato' in df.columns and len(df['data_de_empenho_contrato'].value_counts()) > 0:
            df[consolidacao.DOCUMENTO_DATA] = df['data_de_empenho_contrato']
            df = df.drop(['data_de_empenho_contrato'], axis=1)

        return df
