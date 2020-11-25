from os import path

import logging
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.persistencia.dao import persistir
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_AM_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()

        page = requests.get(self.url)
        soup = BeautifulSoup(page.content, 'lxml')
        tabela = soup.find_all('table')[0]
        ths = tabela.find_all('thead')[0].find_all('th')
        nomes_colunas = [th.get_text() for th in ths]
        tbody = tabela.find_all('tbody')[0]
        trs = tbody.find_all('tr')
        linhas = []

        for tr in trs:
            tds = tr.find_all('td')
            valores = [td.get_text().strip() for td in tds]
            linhas.append(valores)

        df = pd.DataFrame(data=linhas, columns=nomes_colunas)
        persistir(df, 'portal_transparencia', 'contratos', 'AM')

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        logger = logging.getLogger('covidata')
        logger.info('Iniciando consolidação dados Amazonas')
        return self.consolidar_contratos(data_extracao), False

    def consolidar_contratos(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATADO_CNPJ: 'CNPJ/CPFfornecedor',
                            consolidacao.CONTRATADO_DESCRICAO: 'Nomefornecedor',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.CONTRATANTE_DESCRICAO: 'Nome UG',
                            consolidacao.VALOR_CONTRATO: 'Valor atual'}
        df_original = pd.read_excel(
            path.join(config.diretorio_dados, 'AM', 'portal_transparencia', 'contratos.xls'))
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_AM, 'AM', '',
                               data_extracao)
        return df


class PT_Manaus_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()

        downloader = FileDownloader(path.join(config.diretorio_dados, 'AM', 'portal_transparencia', 'Manaus'), self.url,
                                    'PÚBLICA-CONTROLE-PROCESSOS-COMBATE-COVID-19-MATERIAIS.csv')
        downloader.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_materiais_capital(data_extracao), False

    def consolidar_materiais_capital(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'ÓRGÃO',
                            consolidacao.DESPESA_DESCRICAO: 'MATERIAL/SERVIÇO',
                            consolidacao.VALOR_CONTRATO: 'VLR TOTAL CONTRATADO',
                            consolidacao.CONTRATADO_DESCRICAO: 'FORNECEDOR', consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                            consolidacao.DOCUMENTO_NUMERO: 'NOTA DE EMPENHO'}
        df_original = pd.read_csv(
            path.join(config.diretorio_dados, 'AM', 'portal_transparencia', 'Manaus',
                      'PÚBLICA-CONTROLE-PROCESSOS-COMBATE-COVID-19-MATERIAIS.csv'))
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Manaus, 'AM',
                               get_codigo_municipio_por_nome('Manaus', 'AM'), data_extracao,
                               self.pos_processar_materiais_capital)
        return df

    def pos_processar_materiais_capital(self, df):
        # Elimina as 17 últimas linhas, que só contêm totalizadores e legendas
        df.drop(df.tail(17).index, inplace=True)

        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Manaus'
        df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'

        return df
