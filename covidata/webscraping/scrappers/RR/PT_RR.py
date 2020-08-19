import logging
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper
from os import path
import numpy as np

from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_Roraima(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'RR', 'portal_transparencia'), config.url_pt_RR)
        self.arquivos = []

    def _executar(self):
        input_exercicio = self.driver.find_element_by_name('exercicio')
        input_exercicio.send_keys('2020')
        select = Select(self.driver.find_element_by_name('uo'))
        textos = [option.text for option in select.options]

        for i, texto in enumerate(textos):
            if i >= 1:
                select.select_by_visible_text(texto)
                self.driver.find_element_by_tag_name('button').click()
                html = self.driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                link = soup.find('a', {'title': 'XLS'})

                if link:
                    url = link['href']
                    diretorio = path.join(config.diretorio_dados, 'RR', 'portal_transparencia')
                    nome_arquivo = 'arquivo' + str(i) + '.xls'
                    FileDownloader(diretorio, url, nome_arquivo).download()
                    self.arquivos.append(path.join(diretorio, nome_arquivo))

                self.driver.back()
                input_exercicio = self.driver.find_element_by_name('exercicio')
                input_exercicio.send_keys('2020')
                select = Select(self.driver.find_element_by_name('uo'))


class PT_RR_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()

        pt_RR = PortalTransparencia_Roraima()
        pt_RR.download()
        self.arquivos = pt_RR.arquivos

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        df = pd.DataFrame()

        for arquivo in self.arquivos:
            df = df.append(self.__consolidar_arquivo(data_extracao, arquivo))

        return df, False

    def __consolidar_arquivo(self, data_extracao, arquivo):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.NUMERO_PROCESSO: 'Processo',
                            consolidacao.DESPESA_DESCRICAO: 'Histórico do Pedido de Empenho',
                            consolidacao.NUMERO_CONTRATO: 'Contrato',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ/CPF',
                            consolidacao.CONTRATADO_DESCRICAO: 'Credor',
                            consolidacao.VALOR_CONTRATO: 'Valor',
                            consolidacao.VALOR_EMPENHADO: 'Empenhado',
                            consolidacao.VALOR_LIQUIDADO: 'Liquidado',
                            consolidacao.VALOR_PAGO: 'Pago', consolidacao.DATA_INICIO_VIGENCIA: 'Inicio',
                            consolidacao.DATA_FIM_VIGENCIA: 'Témino',
                            consolidacao.CONTRATANTE_DESCRICAO: 'UO',
                            consolidacao.ANO: 'ANO'}

        # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
        colunas_adicionais = ['Situação do Processo', 'Tipo Contrato', 'Situação', 'Aditivos']

        # Lê o arquivo "xls" de contratos baixado como um objeto list utilizando a função "read_html" da biblioteca pandas
        df_original = pd.read_html(path.join(config.diretorio_dados, 'RR', 'portal_transparencia', arquivo),
                                   decimal=',')

        # Chama a função "pre_processar_pt_RR" definida neste módulo
        df = self.pre_processar_pt_RR(df_original)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(colunas_adicionais, df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_RR, 'RR', '',
                               data_extracao, self.pos_processar_pt_RR)

        return df

    def pre_processar_pt_RR(self, df):
        # Lê o segundo elemento do objeto list que se constitui em um objeto pandas DataFrame
        df_titulo = df[0]
        ano = df_titulo.iloc[0, 1]
        unidade_orcamentaria = df_titulo.iloc[1, 1]
        df = df[1]

        # Elimina o nível mais alto dos nomes de colunas de "df"
        df.columns = df.columns.droplevel()

        # Reescreve as colunas de valores monetários inserindo a vírgula designativa de decimais
        for col in np.array(['Empenhado', 'Liquidado', 'Pago']):
            df[col] = df[col].apply(lambda x: str(x)[:-2] + ',' + str(x)[-2:] if ',' not in str(x) else x)

        df['UO'] = unidade_orcamentaria
        df['ANO'] = ano

        return df

    def pos_processar_pt_RR(self, df):
        for i in range(len(df)):
            cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

            if len(cpf_cnpj) == 14:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
            elif len(cpf_cnpj) > 14:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

        return df
