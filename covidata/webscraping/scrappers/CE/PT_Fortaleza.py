import os
import pandas as pd
import time

import logging
from os import path

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_Fortaleza_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_Fortaleza = PortalTransparencia_Fortaleza(config.url_pt_Fortaleza)
        pt_Fortaleza.download()

        # Renomeia o arquivo
        diretorio = path.join(config.diretorio_dados, 'CE', 'portal_transparencia', 'Fortaleza')
        arquivo = os.listdir(diretorio)[0]
        os.rename(path.join(diretorio, arquivo), path.join(diretorio, 'despesas.csv'))

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_despesas_capital(data_extracao), False

    def __consolidar_despesas_capital(self, data_extracao):
        dicionario_dados = {consolidacao.DOCUMENTO_NUMERO: 'N. DO EMPENHO', consolidacao.DOCUMENTO_DATA: 'DATA EMPENHO',
                            consolidacao.CONTRATANTE_DESCRICAO: 'UNIDADE ORCAMENTARIA',
                            consolidacao.UG_DESCRICAO: 'UNIDADE ORCAMENTARIA',
                            consolidacao.VALOR_EMPENHADO: 'VALOR EMPENHO', consolidacao.VALOR_CONTRATO: 'VALOR EMPENHO',
                            consolidacao.CONTRATADO_DESCRICAO: 'CREDOR', consolidacao.CONTRATADO_CNPJ: 'CNPJ / CPF',
                            consolidacao.NUMERO_CONTRATO: 'N. CONTRATO',
                            consolidacao.NUMERO_PROCESSO: 'N. PROCESSO DE AQUISICAO'}
        planilha_original = path.join(config.diretorio_dados, 'CE', 'portal_transparencia', 'Fortaleza', 'despesas.csv')
        df_original = pd.read_csv(planilha_original, sep=';')
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Fortaleza
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'CE', get_codigo_municipio_por_nome('Fortaleza', 'CE'), data_extracao)
        self.pos_processar_despesas_capital(df_original, df)
        return df

    def pos_processar_despesas_capital(self, df_original, df):
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Fortaleza'

        df['temp'] = df[consolidacao.CONTRATANTE_DESCRICAO]
        df[consolidacao.CONTRATANTE_DESCRICAO] = df.apply(
            lambda row: row['temp'][row['temp'].find('- ') + 1:len(row['temp'])], axis=1)
        df[consolidacao.UG_COD] = df.apply(lambda row: row['temp'][0:row['temp'].find('-')], axis=1)
        df[consolidacao.UG_DESCRICAO] = df[consolidacao.CONTRATANTE_DESCRICAO]
        df = df.drop(['temp'], axis=1)

        for i in range(0, len(df)):
            self.__definir_tipo_contratado(df, i)
            self.__definir_situacao_empenho(df_original, df, i)

        df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'

        return df

    def __definir_situacao_empenho(self, df_original, df, i):
        situacao_empenho = df_original.loc[i, 'SITUACAO EMPENHO']
        if situacao_empenho == 'Liquidado':
            df.loc[i, consolidacao.VALOR_LIQUIDADO] = df.loc[i, consolidacao.VALOR_EMPENHADO]
        elif situacao_empenho == 'Pago':
            df.loc[i, consolidacao.VALOR_LIQUIDADO] = df.loc[i, consolidacao.VALOR_EMPENHADO]
            df.loc[i, consolidacao.VALOR_PAGO] = df.loc[i, consolidacao.VALOR_EMPENHADO]

    def __definir_tipo_contratado(self, df, i):
        cpf_cnpj = str(df.loc[i, consolidacao.CONTRATADO_CNPJ])
        if len(cpf_cnpj) == 11:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
        elif len(cpf_cnpj) > 11:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ


class PortalTransparencia_Fortaleza(SeleniumDownloader):

    def __init__(self, url):
        super().__init__(path.join(config.diretorio_dados, 'CE', 'portal_transparencia', 'Fortaleza'), url)

    def _executar(self):
        button = self.driver.find_element_by_id('download')
        button.click()
