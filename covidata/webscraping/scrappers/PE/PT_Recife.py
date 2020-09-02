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
        salvar(dispensas, 'PE')
        return dispensas, True

    def __consolidar_dispensas(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão', consolidacao.UG_DESCRICAO: 'Órgão',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto', consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                            consolidacao.CONTRATADO_DESCRICAO: 'Nome Fornecedor',
                            consolidacao.VALOR_CONTRATO: 'Valor por Fornecedor\n(R$)',
                            consolidacao.DATA_FIM_VIGENCIA: 'Data Vigência',
                            consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'Local de Execução'}
        colunas_adicionais = ['Nº Dispensa', 'Anulação/ Revogação/ Retificação/\nSuspensão',
                              'Data de Empenho/\nContrato',
                              'Data de Empenho\nContrato']
        df_final = pd.DataFrame()

        # Processando arquivos Excel
        planilhas = [y for x in os.walk(path.join(config.diretorio_dados, 'PE', 'portal_transparencia', 'Recife')) for y
                     in
                     glob(os.path.join(x[0], '*.csv'))]

        for planilha_original in planilhas:
            df_original = pd.read_csv(planilha_original)
            df = self.__processar_df_original(colunas_adicionais, data_extracao, df_original, dicionario_dados)
            df_final = df_final.append(df)

        return df_final

    def __processar_df_original(self, colunas_adicionais, data_extracao, df_original, dicionario_dados):
        # Procura pelo cabeçalho:
        mask = np.column_stack([df_original[col].str.contains(colunas_adicionais[0], na=False) for col in df_original])
        df_original.columns = df_original[mask].values.tolist()[0]
        # Remove as linhas anteriores ao cabeçalho
        while df_original.iloc[0, 0] != df_original.columns[0]:
            df_original = df_original.drop(df_original.index[0])
        df_original = df_original.drop(df_original.index[0])
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Recife
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'PE', get_codigo_municipio_por_nome('Recife', 'PE'), data_extracao,
                               self.pos_processar_consolidar_dispensas)
        return df

    def pos_processar_consolidar_dispensas(self, df):
        # Elimina a última linha, que só contém um totalizador
        df = df.drop(df.index[-1])

        df = df.astype({consolidacao.CONTRATADO_CNPJ: np.uint64})
        df = df.astype({consolidacao.CONTRATADO_CNPJ: str})

        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Recife'
        df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'
        df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

        # Unifica colunas com nomes parecidos
        if len(df['DATA DE EMPENHO\nCONTRATO'].value_counts()) > 0:
            df[consolidacao.DOCUMENTO_DATA] = df['DATA DE EMPENHO\nCONTRATO']
        elif len(df['DATA DE EMPENHO/\nCONTRATO'].value_counts()) > 0:
            df[consolidacao.DOCUMENTO_DATA] = df['DATA DE EMPENHO/\nCONTRATO']

        df = df.drop(['DATA DE EMPENHO\nCONTRATO'], axis=1)
        df = df.drop(['DATA DE EMPENHO/\nCONTRATO'], axis=1)

        return df
