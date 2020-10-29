from os import path

import logging
import pandas as pd
import time

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_SC_Contratos_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()

        pt_SC = FileDownloader(path.join(config.diretorio_dados, 'SC', 'portal_transparencia'),
                               config.url_pt_SC_contratos,
                               'contrato_item.xlsx')
        pt_SC.download()

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_pt_SC_contratos(data_extracao), False

    def consolidar_pt_SC_contratos(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {
            consolidacao.CONTRATADO_CNPJ: 'NUIDENTIFICACAOCONTRATADO',
            consolidacao.CONTRATADO_DESCRICAO: 'NMCONTRATADO',
            consolidacao.DESPESA_DESCRICAO: 'DEOBJETOCONTRATO',
            consolidacao.VALOR_CONTRATO: 'VLTOTALATUAL',
            consolidacao.CONTRATANTE_DESCRICAO: 'NMORGAO'}

        # Lê o arquivo "xlsx" de contratos baixado como um objeto pandas DataFrame
        df_original = pd.read_excel(path.join(config.diretorio_dados, 'SC', 'portal_transparencia',
                                              'contrato_item.xlsx'))

        # Chama a função "pre_processar_pt_SC_contratos" definida neste módulo
        df = self.pre_processar_pt_SC_contratos(df_original)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_SC_contratos, 'SC',
                               '',
                               data_extracao)

        return df

    def pre_processar_pt_SC_contratos(self, df):
        # Renomeia as colunas especificadas
        df.rename(index=str,
                  columns={'CDGESTAO': 'Código Gestão',
                           'NMGESTAOCONTRATANTE': 'Nome Gestão Contratante',
                           'NMMODALIDADE': 'Modalidade Licitação',
                           'CDITEMAPRESENTACAO': 'Código Item',
                           'DEITEM': 'Descrição Item',
                           'NMMARCA': 'Marca Item',
                           'DEDESCRICAO': 'Descrição Detalhada Item',
                           'QTITEM': 'Quantidade Item',
                           'VLUNITARIO': 'PU Item',
                           'VLINFORMADO': 'Preço Item',
                           'SGUNIDADEMEDIDA': 'Unidade Item',
                           'NUSERVICOMATERIAL': 'Número Serviço/Material',
                           'DESUBITEM': 'Descrição Subitem',
                           'VLUNITARIOSUBITEM': 'PU Subitem',
                           'QTSUBITEM': 'Quantidade Subitem',
                           'VLINFORMADOSUBITEM': 'Preço Subitem',
                           'SGUNIDADEMEDIDASUBITEM': 'Unidade Subitem',
                           'SIGLAORGAO': 'Sigla Órgão'},
                  inplace=True)

        return df


class PT_SC_Contratos_Despesas(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()

        pt_SC = FileDownloader(path.join(config.diretorio_dados, 'SC', 'portal_transparencia'),
                               config.url_pt_SC_despesas,
                               'analisedespesa.csv')
        pt_SC.download()

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_pt_SC_despesas(data_extracao), False

    def consolidar_pt_SC_despesas(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {
            consolidacao.CONTRATANTE_DESCRICAO: 'descricao',
        }

        # Lê o arquivo "csv" de despesas baixado como um objeto pandas DataFrame
        df_original = pd.read_csv(path.join(config.diretorio_dados, 'SC', 'portal_transparencia',
                                            'analisedespesa.csv'),
                                  sep=';',
                                  encoding='iso-8859-1')

        # Chama a função "pre_processar_pt_SC_despesas" definida neste módulo
        df = self.pre_processar_pt_SC_despesas(df_original)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_SC_despesas, 'SC',
                               '',
                               data_extracao)

        return df

    def pre_processar_pt_SC_despesas(self, df):
        # Renomeia as colunas especificadas
        df.rename(index=str,
                  columns={'vldotacaoinicial': 'Valor Dotação Inicial',
                           'vldotacaoatualizada': 'Valor Dotação Atualizada'},
                  inplace=True)

        return df

class PT_Florianopolis_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_Florianopolis = FileDownloader(
            path.join(config.diretorio_dados, 'SC', 'portal_transparencia', 'Florianopolis'),
            config.url_pt_Florianopolis, 'aquisicoes.csv')
        pt_Florianopolis.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_pt_Florianopolis(data_extracao), False

    def consolidar_pt_Florianopolis(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão Contratante',
                            consolidacao.CONTRATADO_DESCRICAO: 'Nome do Contratado',
                            consolidacao.CONTRATADO_CNPJ: 'CPF/CNPJ do Contratado',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.VALOR_CONTRATO: 'Valor Global'}

        # Lê o arquivo "csv" de despesas baixado como um objeto pandas DataFrame
        df_original = pd.read_csv(path.join(config.diretorio_dados, 'SC', 'portal_transparencia',
                                            'Florianopolis', 'aquisicoes.csv'),
                                  sep=';',
                                  encoding='iso-8859-1')

        # Chama a função "pre_processar_pt_Florianopolis" definida neste módulo
        df = self.pre_processar_pt_Florianopolis(df_original)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Florianopolis, 'SC',
                               get_codigo_municipio_por_nome('Florianópolis', 'SC'), data_extracao)

        return df

    def pre_processar_pt_Florianopolis(self, df):
        # Renomeia as colunas especificadas
        df.rename(index=str,
                  columns={'Número Dispensa de Licitação': 'Número Dispensa',
                           'Local de Entrega da Prestação de Serviço': 'Local Entrega',
                           'Unidade': 'Unidade Objeto',
                           'Quantidade': 'Quantidade Objeto',
                           'Data de Assinatura Instumento Contratual': 'Data Assinatura Contrato',
                           'Processo de Contratação ou Aquisição  para Download': 'Número Processo',
                           'Instumento Contratual': 'Número Contrato',
                           'Modalidade de Licitação': 'Modalidade Licitação'},
                  inplace=True)

        return df

