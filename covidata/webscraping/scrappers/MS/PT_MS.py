from glob import glob
from os import path

import logging
import os
import pandas as pd
import re
import requests
import time
from bs4 import BeautifulSoup

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.json.parser import JSONParser
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_MS_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')

        logger.info('Portal de transparência estadual...')

        page = requests.get(self.url)
        soup = BeautifulSoup(page.content, 'html.parser')
        links = soup.find_all('a')
        p = re.compile(r'.*compras.+.csv')
        links = [link.attrs['href'] for link in links if
                 p.match(link.attrs['href']) and not 'itens' in link.attrs['href']]
        diretorio = config.diretorio_dados.joinpath('MS').joinpath('portal_transparencia')

        if not path.exists(diretorio):
            os.makedirs(diretorio)

        for link in links:
            indice = link.rfind('/')
            downloader = FileDownloader(diretorio, link, link[indice + 1:])
            downloader.download()

    def consolidar(self, data_extracao):
        return self.__consolidar_compras_emergenciais(data_extracao), False

    def __consolidar_compras_emergenciais(self, data_extracao):
        # Processando arquivos Excel
        planilhas = [y for x in os.walk(path.join(config.diretorio_dados, 'MS', 'portal_transparencia')) for y
                     in glob(os.path.join(x[0], '*.csv'))]
        df_final = pd.DataFrame()

        for planilha in planilhas:
            df = pd.read_csv(planilha, sep=';', index_col=False)
            df_consolidado = self.__consolidar_planilha(data_extracao, df)
            df_final = df_final.append(df_consolidado)

        return df_final

    def __consolidar_planilha(self, data_extracao, df):
        df_consolidado = pd.DataFrame(
            columns=[consolidacao.FONTE_DADOS, consolidacao.DATA_EXTRACAO_DADOS, consolidacao.ESFERA,
                     consolidacao.UF, consolidacao.COD_IBGE_MUNICIPIO, consolidacao.MUNICIPIO_DESCRICAO])
        # Colunas não são uniformes
        df_consolidado[consolidacao.CONTRATADO_CNPJ] = df['CPF/CNPJ']
        df_consolidado[consolidacao.CONTRATADO_DESCRICAO] = df['Credor']
        df_consolidado[consolidacao.CONTRATANTE_DESCRICAO] = df['Órgão']
        df_consolidado[consolidacao.DESPESA_DESCRICAO] = df['Objeto']
        colunas = list(df.columns)
        if 'Valor' in colunas:
            df_consolidado[consolidacao.VALOR_CONTRATO] = df['Valor']
        elif 'Valor Total' in colunas:
            df_consolidado[consolidacao.VALOR_CONTRATO] = df['Valor Total']
        elif 'Valor Total Da Compra' in colunas:
            df_consolidado[consolidacao.VALOR_CONTRATO] = df['Valor Total Da Compra']
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_MS
        df_consolidado[consolidacao.FONTE_DADOS] = fonte_dados
        df_consolidado[consolidacao.DATA_EXTRACAO_DADOS] = data_extracao
        df_consolidado[consolidacao.UF] = 'MS'
        df_consolidado[consolidacao.ESFERA] = consolidacao.ESFERA_ESTADUAL
        # Remove espaços extras do início e do final das colunas do tipo string
        df_consolidado = df_consolidado.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        return df_consolidado


class PT_CampoGrande_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()

        PortalTransparenciaCampoGrande().parse()

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        despesas_capital = self.__consolidar_despesas_capital(data_extracao)
        return despesas_capital, False

    def __consolidar_despesas_capital(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'orgao',
                            consolidacao.CONTRATADO_DESCRICAO: 'nomefornecedor',
                            consolidacao.CONTRATADO_CNPJ: 'fornecedor',
                            consolidacao.DESPESA_DESCRICAO: 'itemclassificacaodespesa'}
        planilha_original = path.join(config.diretorio_dados, 'MS', 'portal_transparencia', 'Campo Grande',
                                      'despesas.xlsx')
        df_original = pd.read_excel(planilha_original, header=1)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_CampoGrande
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'MS', get_codigo_municipio_por_nome('Campo Grande', 'MS'), data_extracao,
                               self.pos_processar_despesas_capital)
        return df

    def pos_processar_despesas_capital(self, df):
        df = df.rename(columns={'PROCESSO DE ORIGEM': 'NÚMERO PROCESSO'})
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Campo Grande'
        return df


class PortalTransparenciaCampoGrande(JSONParser):
    def __init__(self):
        super().__init__(config.url_pt_CampoGrande, 'num', 'despesas', 'portal_transparencia', 'MS', 'Campo Grande')

    def _get_elemento_raiz(self, conteudo):
        return conteudo['data']
