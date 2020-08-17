import json
import logging
import os
import time

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.persistencia.dao import persistir
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.json.parser import JSONParser
from covidata.webscraping.scrappers.scrapper import Scraper
import pandas as pd
from os import path


class PT_AC_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()
        PortalTransparenciaAcre().parse()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_portal_transparencia_estadual(data_extracao)

    def __consolidar_portal_transparencia_estadual(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.ANO: 'ANOEMPENHO',
                            consolidacao.VALOR_EMPENHADO: 'VALOREMPENHADO',
                            consolidacao.CONTRATADO_CNPJ: 'CPFCNPJCREDOR',
                            consolidacao.CONTRATADO_DESCRICAO: 'RAZAOSOCIAL',
                            consolidacao.DOCUMENTO_NUMERO: 'NUMEROEMPENHO',
                            consolidacao.DOCUMENTO_DATA: 'DATAEMPENHO',
                            consolidacao.FONTE_RECURSOS_COD: 'FONTERECURSO'}

        # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
        colunas_adicionais = ['CLASSECREDOR']

        # Lê o arquivo "xlsx" de empenhos baixado como um objeto pandas DataFrame
        df_original = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'portal_transparencia', 'empenhos.xls'),
                                    header=4)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_AC, 'AC', '',
                               data_extracao, self.pos_processar_portal_transparencia_estadual)

        return df

    def pos_processar_portal_transparencia_estadual(self, df):
        df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'

        for i in range(len(df)):
            cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

            if len(str(cpf_cnpj)) >= 14:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
            else:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF

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

