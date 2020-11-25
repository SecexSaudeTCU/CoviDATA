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
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_MA_Scraper(Scraper):
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
        persistir(df, 'portal_transparencia', 'contratos', 'MA')

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_portal_transparencia_estado(data_extracao), False

    def __consolidar_portal_transparencia_estado(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATADO_DESCRICAO: 'contratado',
                            consolidacao.CONTRATADO_CNPJ: ' cnpj_cpf_contratado',
                            consolidacao.CONTRATANTE_DESCRICAO: 'orgao_contratante',
                            consolidacao.DESPESA_DESCRICAO: ' descricao_servico',
                            consolidacao.VALOR_CONTRATO: 'valor_total'}
        planilha_original = path.join(config.diretorio_dados, 'MA', 'portal_transparencia', 'contratos.xls')
        df_original = pd.read_excel(planilha_original, header=4)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_MA
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               fonte_dados, 'MA', '', data_extracao)
        return df


class PT_SaoLuis_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        page = requests.get(config.url_pt_SaoLuis)
        soup = BeautifulSoup(page.content, 'html.parser')
        tabela = soup.find_all('table')[0]
        tbody = tabela.find_all('tbody')[0]
        linhas = tbody.find_all('tr')
        titulos = tabela.find_all('th')
        colunas = ['Link contrato']
        colunas += [titulo.get_text() for titulo in titulos]
        lista_linhas = []

        for linha in linhas:
            data = linha.find_all("td")
            nova_linha = [data[1].find_all('a')[0].attrs['href']]
            nova_linha += [data[i].get_text() for i in range(len(data))]
            lista_linhas.append(nova_linha)

        df = pd.DataFrame(lista_linhas, columns=colunas)
        persistir(df, 'portal_transparencia', 'contratacoes', 'MA', 'São Luís')
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_portal_transparencia_capital(data_extracao), False

    def __consolidar_portal_transparencia_capital(self, data_extracao):
        dicionario_dados = {consolidacao.VALOR_CONTRATO: 'Valor do Contrato (R$)',
                            consolidacao.DESPESA_DESCRICAO: 'Descrição', consolidacao.CONTRATADO_DESCRICAO: 'Empresa',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                            consolidacao.CONTRATANTE_DESCRICAO: 'Unidade Contratante'}
        planilha_original = path.join(config.diretorio_dados, 'MA', 'portal_transparencia', 'São Luís',
                                      'contratacoes.xls')
        df_original = pd.read_excel(planilha_original, header=4)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_SaoLuis
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'MA', get_codigo_municipio_por_nome('São Luís', 'MA'), data_extracao,
                               self.pos_processar_portal_transparencia_capital)
        return df

    def pos_processar_portal_transparencia_capital(self, df):
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'São Luís'
        df = df.rename(columns={'Nº DO PROCESSO': 'Nº PROCESSO'})

        return df
