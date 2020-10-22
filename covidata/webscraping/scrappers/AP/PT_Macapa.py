import os
import pandas as pd
import time

import logging

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from os import path

from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_Macapa_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')

        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_Macapa = PortalTransparencia_Macapa()
        pt_Macapa.download()

        # Renomeia o arquivo
        diretorio = path.join(config.diretorio_dados, 'AP', 'portal_transparencia', 'Macapa')
        arquivo = os.listdir(diretorio)[0]
        os.rename(path.join(diretorio, arquivo), path.join(diretorio, 'transparencia.xlsx'))

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        logger = logging.getLogger('covidata')
        logger.info('Iniciando consolidação dados Amapá')
        return self.consolidar_contratacoes_capital(data_extracao), False

    def consolidar_contratacoes_capital(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATADO_CNPJ: 'Contratado - CNPJ / CPF',
                            consolidacao.DESPESA_DESCRICAO: 'Descrição de bem ou serviço',
                            consolidacao.ITEM_EMPENHO_QUANTIDADE: 'Quantidade',
                            consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'Valor Unitário',
                            consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'Valor Total',
                            consolidacao.CONTRATANTE_DESCRICAO: 'Órgão Contratante',
                            consolidacao.UG_DESCRICAO: 'Órgão Contratante',
                            consolidacao.VALOR_CONTRATO: 'Valor contratado', consolidacao.VALOR_PAGO: 'Valor pago',
                            consolidacao.MOD_APLIC_DESCRICAO: 'Forma / modalidade',
                            consolidacao.DATA_CELEBRACAO: 'Data de Celebração / Publicação',
                            consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'Local de execução'}
        planilha_original = path.join(config.diretorio_dados, 'AP', 'portal_transparencia', 'Macapa',
                                      'transparencia.xlsx')
        df_original = pd.read_excel(planilha_original, header=1)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Macapa
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'AP', get_codigo_municipio_por_nome('Macapá', 'AP'), data_extracao,
                               self.pos_processar_contratacoes_capital)
        return df

    def pos_processar_contratacoes_capital(self, df):
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Macapá'

        df['temp'] = df[consolidacao.CONTRATANTE_DESCRICAO]
        df[consolidacao.CONTRATANTE_DESCRICAO] = df.apply(lambda row: row['temp'][0:row['temp'].find('-')], axis=1)
        df[consolidacao.LOCAL_EXECUCAO_OU_ENTREGA] = df.apply(
            lambda row: row['temp'][row['temp'].find('-') + len(' LOCAL DE EXECUÇÃO: '):len(row['temp'])], axis=1)
        df[consolidacao.UG_DESCRICAO] = df[consolidacao.CONTRATANTE_DESCRICAO]
        df = df.drop(['temp'], axis=1)

        df['temp'] = df[consolidacao.CONTRATADO_CNPJ]
        df[consolidacao.CONTRATADO_DESCRICAO] = df.apply(lambda row: row['temp'][0:row['temp'].find('/')], axis=1)
        df[consolidacao.CONTRATADO_CNPJ] = df.apply(lambda row: row['temp'][row['temp'].find('/') + 1:len(row['temp'])],
                                                    axis=1)
        df = df.drop(['temp'], axis=1)

        for i in range(0, len(df)):
            cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ].strip()

            if len(cpf_cnpj) == 14:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
            elif len(cpf_cnpj) > 14:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

        return df


class PortalTransparencia_Macapa(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'AP', 'portal_transparencia', 'Macapa'),
                         config.url_pt_Macapa)

    def _executar(self):
        button = self.driver.find_element_by_class_name('buttons-excel')
        button.click()
