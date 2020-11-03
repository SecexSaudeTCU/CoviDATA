import json
import logging
import os
import time

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.persistencia.dao import persistir
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.json.parser import JSONParser
from covidata.webscraping.scrappers.scrapper import Scraper
import pandas as pd
from os import path

from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_AC_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()
        PortalTransparenciaAcre().parse()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_portal_transparencia_estadual(data_extracao), False

    def __consolidar_portal_transparencia_estadual(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATADO_CNPJ: 'CPFCNPJCREDOR',
                            consolidacao.CONTRATADO_DESCRICAO: 'RAZAOSOCIAL',
                            consolidacao.DOCUMENTO_NUMERO: 'NUMEROEMPENHO',
                            consolidacao.DOCUMENTO_DATA: 'DATAEMPENHO'}

        # Lê o arquivo "xlsx" de empenhos baixado como um objeto pandas DataFrame
        df_original = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'portal_transparencia', 'empenhos.xls'),
                                    header=4)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_AC, 'AC', '',
                               data_extracao, self.pos_processar_portal_transparencia_estadual)

        return df

    def pos_processar_portal_transparencia_estadual(self, df):
        df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'

        return df


class PortalTransparenciaAcre(JSONParser):
    def __init__(self):
        super().__init__(config.url_pt_AC, 'NUMEROEMPENHO', 'empenhos', 'portal_transparencia', 'AC')

    def parse(self):
        """
        Executa o parsing do arquivo JSON.  Executa o download de um arquivo de dados em formato JSON e armazena as
        informações resultantes em um local identificado de acordo com as informações recebidas pelo construtor da
        classe.
        :return:
        """
        nome_arquivo = self.nome_dados + '.json'
        downloader = FileDownloader(self.diretorio, self.url, nome_arquivo)
        downloader.download()

        with open(os.path.join(downloader.diretorio_dados, downloader.nome_arquivo)) as json_file:
            dados = json.load(json_file)
            conteudo = dados['data']['rows']
            colunas = dados['data']['cols']
            colunas_df = []

            for coluna in colunas:
                colunas_df.append(coluna['name'])

            linhas_df = []

            for elemento in conteudo:
                linhas_df.append(elemento)

            df = pd.DataFrame(linhas_df, columns=colunas_df)

            persistir(df, self.fonte, self.nome_dados, self.uf)

    def _get_elemento_raiz(self, conteudo):
        return None


class PT_RioBranco_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_RioBranco = PortalTransparencia_RioBranco(config.url_pt_RioBranco)
        pt_RioBranco.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_portal_transparencia_capital(data_extracao), False

    def __consolidar_portal_transparencia_capital(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATADO_CNPJ:'CPF/CNPJ',
                            consolidacao.CONTRATADO_DESCRICAO: 'Nome',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.CONTRATANTE_DESCRICAO: 'Secretaria', consolidacao.VALOR_CONTRATO: 'Valor Total'}
        df_original = pd.read_excel(
            path.join(config.diretorio_dados, 'AC', 'portal_transparencia', 'Rio Branco', 'webexcel.xls'), header=11)
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_RioBranco, 'AC',
                               get_codigo_municipio_por_nome('Rio Branco', 'AC'), data_extracao,
                               self.pos_processar_portal_transparencia_capital)
        return df

    def pos_processar_portal_transparencia_capital(self, df):
        # Elimina as sete últimas linhas
        df.drop(df.tail(7).index, inplace=True)
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'RIO BRANCO'
        return df


class PortalTransparencia_RioBranco(SeleniumDownloader):
    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'AC', 'portal_transparencia', 'Rio Branco'), url)

    def _executar(self):
        button = self.driver.find_element_by_partial_link_text('EXCEL')
        button.click()