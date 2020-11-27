from datetime import date
from os import path

import logging
import numpy as np
import pandas as pd
import requests
import time

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_RO_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()
        pt = FileDownloader(path.join(config.diretorio_dados, 'RO', 'portal_transparencia'), config.url_pt_RO,
                            'Despesas.CSV')
        pt.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_pt_RO(data_extracao), False

    def consolidar_pt_RO(self, data_extracao):
        dicionario_dados = {consolidacao.DOCUMENTO_DATA: 'DataDocumento',
                            consolidacao.DESPESA_DESCRICAO: 'NomDespesa',
                            consolidacao.CONTRATANTE_DESCRICAO: 'NomOrgao',
                            consolidacao.DOCUMENTO_NUMERO: 'documento', consolidacao.CONTRATADO_CNPJ: 'DocCredor',
                            consolidacao.CONTRATADO_DESCRICAO: 'Credor'}
        planilha_original = path.join(config.diretorio_dados, 'RO', 'portal_transparencia', 'Despesas.CSV')
        df_original = pd.read_csv(planilha_original, sep=';', header=0, encoding='utf_16_le')
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_RO
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               fonte_dados, 'RO', '', data_extracao, self.pos_consolidar_pt_RO)
        return df

    def pos_consolidar_pt_RO(self, df):
        # Remove notação científica
        df = df.astype({consolidacao.CONTRATADO_CNPJ: np.uint64})
        df = df.astype({consolidacao.CONTRATADO_CNPJ: str})
        df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'

        for i in range(0, len(df)):
            tamanho = len(df.loc[i, consolidacao.CONTRATADO_CNPJ])

            if tamanho < 14:
                df.loc[i, consolidacao.CONTRATADO_CNPJ] = '0' * (14 - tamanho) + df.loc[i, consolidacao.CONTRATADO_CNPJ]

        return df


class PT_PortoVelho_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        # pt_PortoVelho = PortalTransparencia_PortoVelho()
        # pt_PortoVelho.download()

        data_atual = date.today()
        aParametros = {'iExercicio': data_atual.year,
                       'dtDataImportacao': f'{data_atual.year}-{data_atual.month}-{data_atual.day}',
                       'sViewAtual': 'credores', 'iNivel': 3, 'dtInicio': '', 'dtFim': '',
                       'aHistorico': [{'descricao': 'Descrição', 'valor_empenhado': 'Valor Empenhado',
                                       'valor_anulado': 'Valor Anulado', 'valor_liquidado': 'Valor Liquidado',
                                       'valor_pago': 'Valor Pago'}], 'iIdLink': 3}
        r = requests.post(config.url_pt_PortoVelho,
                          data={'aParametros': aParametros, '_search': False, 'nd': '1606513436451', 'rows': 999,
                                'page': 1, 'sidx': 'nome', 'sord': 'asc'})
        conteudo_json = r.json()
        rows = conteudo_json['rows']
        colunas = ['cpf_cnpj', 'credor']
        linhas = [[(row['cell'][0]), (row['cell'][1])] for row in rows]

        df = pd.DataFrame(data=linhas, columns=colunas)
        df.to_excel(path.join(config.diretorio_dados, 'RO', 'portal_transparencia', 'PortoVelho', 'despesas.xlsx'))

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_pt_PortoVelho(data_extracao), False

    def consolidar_pt_PortoVelho(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATADO_CNPJ: 'cpf_cnpj',
                            consolidacao.CONTRATADO_DESCRICAO: 'credor'}

        # Lê o arquivo "csv" de despesas baixado como um objeto pandas DataFrame
        df_original = pd.read_excel(path.join(config.diretorio_dados, 'RO', 'portal_transparencia',
                                              'PortoVelho', 'despesas.xlsx'))

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_PortoVelho, 'RO',
                               get_codigo_municipio_por_nome('Porto Velho', 'RO'), data_extracao)
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Porto Velho'
        return df


class PortalTransparencia_PortoVelho(SeleniumDownloader):

    # Sobrescreve o construtor da class "SeleniumDownloader"
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'RO', 'portal_transparencia', 'PortoVelho'),
                         config.url_pt_PortoVelho)

    # Implementa localmente o método interno e vazio da class "SeleniumDownloader"
    def _executar(self):
        # Seleciona o elemento da lista "Despesas por Credor / Instituição"
        self.driver.find_element_by_xpath('//*[@id="consulta_dados"]/fieldset/div/ul/li[2]/a').click()

        # On hold por 5 segundos
        time.sleep(5)

        # Seleciona o botão que tem como símbolo um arquivo "csv"
        self.driver.find_element_by_xpath('//*[@id="main_content"]/div[3]/span[3]').click()
