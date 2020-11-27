from os import path

import logging
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.persistencia.dao import persistir
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_RN_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/50.0.2661.102 Safari/537.36'}
        # page = requests.get(config.url_pt_RN, headers=headers, verify=False)
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        page = session.get(config.url_pt_RN, headers=headers, verify=False)

        soup = BeautifulSoup(page.content, 'html.parser')
        tabela = soup.find_all('table')[1]
        titulos_colunas = tabela.find_all('thead')[0].find_all('th')
        colunas = [titulo_coluna.get_text() for titulo_coluna in titulos_colunas]

        linhas = tabela.find_all('tbody')[0].find_all('tr')
        linhas_df = []

        for linha in linhas:
            tds = linha.find_all('td')
            valores = [td.get_text().strip() for td in tds]
            linhas_df.append(valores)

        df = pd.DataFrame(linhas_df, columns=colunas)
        persistir(df, 'portal_transparencia', 'compras_e_servicos', 'RN')

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_pt_RN(data_extracao), False

    def consolidar_pt_RN(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Contratante',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.CONTRATADO_DESCRICAO: 'Contratado (a)',
                            consolidacao.VALOR_CONTRATO: 'Valor do Contrato (R$)',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ/CPF'}

        df_original = pd.read_excel(
            path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'compras_e_servicos.xls'), header=4)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_RN, 'RN', '',
                               data_extracao)
        return df


class PT_Natal_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')

        logger.info('Portal de transparência da capital...')
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
        persistir(df, 'portal_transparencia', 'contratos', 'RN', cidade='Natal')

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_pt_Natal(data_extracao), False

    def consolidar_pt_Natal(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATADO_DESCRICAO: 'Fornecedor',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ/CPF Fornecedor',
                            consolidacao.CONTRATANTE_DESCRICAO: 'Orgão Contratante',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto', consolidacao.VALOR_CONTRATO: 'Valor Total'}

        # Lê o arquivo "xlsx" de nome de despesas baixado como um objeto pandas DataFrame
        df_original = pd.read_excel(
            path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'Natal', 'contratos.xls'),
            header=4)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Natal, 'RN',
                               get_codigo_municipio_por_nome('Natal', 'RN'), data_extracao)
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Natal'
        return df
