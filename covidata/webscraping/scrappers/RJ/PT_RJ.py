from os import path

import logging
import numpy as np
import pandas as pd
import time

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_RioDeJaneiro_Favorecidos_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_Rio_favorecidos = FileDownloader(
            path.join(config.diretorio_dados, 'RJ', 'portal_transparencia', 'Rio de Janeiro'),
            config.url_pt_Rio_favorecidos, 'Open_Data_Favorecidos_Covid19_2020.xlsx')
        pt_Rio_favorecidos.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        favorecidos_capital = self.__consolidar_favorecidos_capital(data_extracao)
        return favorecidos_capital, False

    def __consolidar_favorecidos_capital(self, data_extracao):
        dicionario_dados = {
            consolidacao.DOCUMENTO_NUMERO: 'Número do empenho',
            consolidacao.CONTRATADO_DESCRICAO: 'Favorecido',
            consolidacao.CONTRATADO_CNPJ: 'Código favorecido',
            consolidacao.CONTRATANTE_DESCRICAO: 'Descrição do órgão executor',
            consolidacao.DESPESA_DESCRICAO: 'Descrição da natureza'
        }
        planilha_original = path.join(config.diretorio_dados, 'RJ', 'portal_transparencia', 'Rio de Janeiro',
                                      'Open_Data_Favorecidos_Covid19_2020.xlsx')
        df_original = pd.read_excel(planilha_original)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Rio_favorecidos
        codigo_municipio_ibge = get_codigo_municipio_por_nome('Rio de Janeiro', 'RJ')
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'RJ', codigo_municipio_ibge, data_extracao,
                               self.pos_processar_favorecidos_capital)
        return df

    def pos_processar_favorecidos_capital(self, df):
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Rio de Janeiro'

        # Remove a notação científica
        df[consolidacao.CONTRATADO_CNPJ] = df[consolidacao.CONTRATADO_CNPJ].astype(np.int64)
        df[consolidacao.CONTRATADO_CNPJ] = df[consolidacao.CONTRATADO_CNPJ].astype(str)

        df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'
        df = df.rename(
            columns={'UNIDADE ORÇAMENTÁRIA EXECUTORA': 'UO', 'DESCRIÇÃO DA UNIDADE ORÇAMENTÁRIA EXECUTORA': 'NOMEUO',
                     'MODALIDADE': 'MODALIDADE DE LICITAÇÃO', 'AGENCIA BANCO': 'AGENCIA',
                     'DESCRIÇÃO DA NATUREZA': 'DESCRIÇÃO DA NATUREZA DA DESPESA'})
        return df


class PT_RioDeJaneiro_Contratos_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_Rio_contratos = FileDownloader(
            path.join(config.diretorio_dados, 'RJ', 'portal_transparencia', 'Rio de Janeiro'),
            config.url_pt_Rio_contratos, 'Open_Data_Contratos_Covid19_2020.xlsx')
        pt_Rio_contratos.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_contratos_capital(data_extracao), False

    def __consolidar_contratos_capital(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATADO_CNPJ: 'Código favorecido',
                            consolidacao.CONTRATADO_DESCRICAO: 'Favorecido',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.CONTRATANTE_DESCRICAO: 'Descrição do órgão executor',
                            consolidacao.VALOR_CONTRATO: 'Valor atualizado do instrumento'}
        planilha_original = path.join(config.diretorio_dados, 'RJ', 'portal_transparencia', 'Rio de Janeiro',
                                      'Open_Data_Contratos_Covid19_2020.xlsx')
        df_original = pd.read_excel(planilha_original)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Rio_contratos
        codigo_municipio_ibge = get_codigo_municipio_por_nome('Rio de Janeiro', 'RJ')
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'RJ', codigo_municipio_ibge, data_extracao,
                               self.pos_processar_contratos_capital)
        return df

    def pos_processar_contratos_capital(self, df):
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Rio de Janeiro'

        # Remove a notação científica
        df[consolidacao.CONTRATADO_CNPJ] = df[consolidacao.CONTRATADO_CNPJ].astype(np.int64)
        df[consolidacao.CONTRATADO_CNPJ] = df[consolidacao.CONTRATADO_CNPJ].astype(str)

        df = df.rename(
            columns={'UNIDADE ORÇAMENTÁRIA EXECUTORA': 'UO', 'DESCRIÇÃO DA UNIDADE ORÇAMENTÁRIA EXECUTORA': 'NOMEUO'})
        return df


class PT_RioDeJaneiro_DespesasPorAto_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_Rio_despesas_por_ato = FileDownloader(
            path.join(config.diretorio_dados, 'RJ', 'portal_transparencia', 'Rio de Janeiro'),
            config.url_pt_Rio_despesas_por_ato, '_arquivos_Open_Data_Desp_Ato_Covid19_2020.txt')
        pt_Rio_despesas_por_ato.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_despesas_capital(data_extracao), False

    def __consolidar_despesas_capital(self, data_extracao):
        dicionario_dados = {
            consolidacao.CONTRATANTE_DESCRICAO: 'NomeOrgao', consolidacao.CONTRATADO_CNPJ: 'Credor',
            consolidacao.CONTRATADO_DESCRICAO: 'NomeCredor',
            consolidacao.VALOR_CONTRATO: 'Valor',
            consolidacao.DOCUMENTO_NUMERO: 'EmpenhoExercicio',
            consolidacao.DOCUMENTO_DATA: 'Data', consolidacao.TIPO_DOCUMENTO: 'TipoAto',
            consolidacao.DESPESA_DESCRICAO: 'ObjetoContrato',
        }
        planilha_original = path.join(config.diretorio_dados, 'RJ', 'portal_transparencia', 'Rio de Janeiro',
                                      '_arquivos_Open_Data_Desp_Ato_Covid19_2020.txt')
        df_original = pd.read_csv(planilha_original, sep=';', encoding='ISO-8859-1')
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Rio_despesas_por_ato
        codigo_municipio_ibge = get_codigo_municipio_por_nome('Rio de Janeiro', 'RJ')
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'RJ', codigo_municipio_ibge, data_extracao, self.pos_processar)

        return df

    def pos_processar(self, df):
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Rio de Janeiro'

        # Remove a notação científica
        df[consolidacao.CONTRATADO_CNPJ] = df[consolidacao.CONTRATADO_CNPJ].astype(np.int64)
        df[consolidacao.CONTRATADO_CNPJ] = df[consolidacao.CONTRATADO_CNPJ].astype(str)

        return df
