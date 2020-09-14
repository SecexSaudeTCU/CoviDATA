import calendar
import datetime
import locale
import logging
import os
import platform
import time
from glob import glob
from os import path

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.downloader import download, FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_SP_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        downloader = FileDownloader(path.join(config.diretorio_dados, 'SP', 'portal_transparencia'), self.url,
                                    'COVID.csv')
        downloader.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_PT(data_extracao), False

    def consolidar_PT(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Secretaria', consolidacao.UG_DESCRICAO: 'Secretaria',
                            consolidacao.NUMERO_PROCESSO: 'Número do Processo',
                            consolidacao.CONTRATADO_DESCRICAO: 'Contratada / Conveniada',
                            consolidacao.CONTRATADO_CNPJ: 'CPF / CNPJ / CGC',
                            consolidacao.DESPESA_DESCRICAO: 'Descrição Processo',
                            consolidacao.ITEM_EMPENHO_DESCRICAO: 'Finalidade/Item',
                            consolidacao.ITEM_EMPENHO_QUANTIDADE: 'Quantidade',
                            consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'Valor Unitário',
                            consolidacao.DOCUMENTO_NUMERO: 'Nota de Empenho', consolidacao.VALOR_EMPENHADO: 'Empenho',
                            consolidacao.FONTE_RECURSOS_COD: 'Código Fonte Recurso',
                            consolidacao.FONTE_RECURSOS_DESCRICAO: 'Código Nome Fonte Detalhada',
                            consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'Local Entrega'}
        colunas_adicionais = ['Modalidade de Contratação', 'Data da Movimentação', 'Tipo de Pagamento',
                              'Número de Pagamento', 'Valor NE', 'Valor NL', 'Valor NP', 'Valor OB']
        df_original = pd.read_csv(path.join(config.diretorio_dados, 'SP', 'portal_transparencia', 'COVID.csv'),
                                  encoding='ISO-8859-1', sep=';')
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_SP, 'SP',
                               None, data_extracao, self.pos_processar_PT)
        return df

    def pos_processar_PT(self, df):
        df = df.astype({consolidacao.CONTRATADO_CNPJ: str})

        for i in range(0, len(df)):
            cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

            if len(cpf_cnpj) == 14:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
            elif len(cpf_cnpj) == 18:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

        return df


class PT_SaoPaulo_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência municipal...')
        start_time = time.time()

        if 'Windows' in platform.system():
            locale.setlocale(locale.LC_TIME, "pt-BR")
        else:
            # TODO: Testar no Linux
            locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")

        mes_inicial = 3
        mes_atual = datetime.datetime.now().month
        meses = []

        if mes_atual > mes_inicial:
            meses = [self.__get_nome_mes(i) for i in range(mes_inicial, mes_atual + 1)]
        elif mes_atual <= mes_inicial:
            # de um ano para o outro
            meses = [self.__get_nome_mes(i) for i in range(mes_inicial, 13)]
            meses = meses + [self.__get_nome_mes(i) for i in range(1, mes_atual + 1)]

        self.__baixar_arquivos(meses)

        # Obtém objeto list dos arquivos armazenados no path passado como argumento para a função nativa "glob"
        list_files = glob(path.join(config.diretorio_dados, 'SP', 'portal_transparencia', 'São Paulo/*'))

        df = pd.DataFrame(
            columns=['Número Processo', 'Data Publicação', 'Modalidade Licitação', 'Órgão', 'Contratada', 'CNPJ',
                     'Objeto', 'Item', 'Quantidade', 'Unidade Item', 'PU Item', 'Preço Item',
                     'Valor Total', 'Prazo', 'Unidade tempo', 'Local Entrega', 'Texto Publicado'])

        for file_name in list_files:

            if file_name.endswith('xls'):
                frame = pd.read_excel(file_name)

                frame.rename(columns=lambda x: x.strip(), inplace=True)

                frame.rename(index=str,
                             columns={'Processo': 'Número Processo',
                                      'Data de Publicação': 'Data Publicação',
                                      'Modalidade': 'Modalidade Licitação',
                                      'Descrição': 'Unidade Item',
                                      'Valor Unitário': 'PU Item',
                                      'Valor total Item': 'Preço Item',
                                      'Valor Total Contrato': 'Valor Total',
                                      'Local': 'Local Entrega',
                                      'Texto Públicado': 'Texto Publicado'},
                             inplace=True)

                frame = frame[['Número Processo', 'Data Publicação', 'Modalidade Licitação', 'Órgão', 'Contratada',
                               'CNPJ', 'Objeto', 'Item', 'Quantidade', 'Unidade Item', 'PU Item', 'Preço Item',
                               'Valor Total', 'Prazo', 'Unidade tempo', 'Local Entrega', 'Texto Publicado']]

                df = pd.concat([df, frame], sort=False)

                os.unlink(file_name)

        diretorio = path.join(config.diretorio_dados, 'SP', 'portal_transparencia', 'São Paulo')
        os.makedirs(diretorio)
        df.to_excel(path.join(diretorio, 'Portal_Transparencia_SP_capital.xlsx'), index=False)

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def __baixar_arquivos(self, meses):
        url = config.url_pt_SaoPaulo
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        tags = soup.find_all('strong')

        for tag in tags:
            for mes in meses:
                if mes in tag.text:
                    self.__baixar_arquivo_mensal(mes, tag)

    def __baixar_arquivo_mensal(self, mes, tag):
        links = tag.find_all('a', string='Excel')

        # Solução de contorno para o caso do mês de junho
        if len(links) == 0:
            links = tag.find_all('a', string='Excel ')

        if len(links) == 0:
            for sibling in tag.next_siblings:
                if isinstance(sibling, Tag):
                    links = sibling.find_all('a', string='Excel')
                    if len(links) > 0:
                        break
        if len(links) > 0:
            link = links[0]
            url = link.attrs['href']
            diretorio = path.join(config.diretorio_dados, 'SP', 'portal_transparencia', 'São Paulo')
            download(url, diretorio, path.join(diretorio, mes + '.xls'))

    def consolidar(self, data_extracao):
        return self.consolidar_pt_SP_capital(data_extracao), False

    def consolidar_pt_SP_capital(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão',
                            consolidacao.CONTRATADO_DESCRICAO: 'Contratada',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.VALOR_CONTRATO: 'Valor Total'}

        # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
        colunas_adicionais = ['Número Processo', 'Data Publicação', 'Modalidade Licitação', 'Item',
                              'Quantidade', 'Unidade Item', 'PU Item', 'Prazo', 'Unidade tempo',
                              'Local Entrega', 'Texto Publicado']

        # Lê o arquivo "xlsx" de licitações baixado como um objeto pandas DataFrame
        df_original = pd.read_excel(path.join(config.diretorio_dados, 'SP', 'portal_transparencia', 'São Paulo',
                                              'Portal_Transparencia_SP_capital.xlsx'))

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_SaoPaulo, 'SP',
                               get_codigo_municipio_por_nome('São Paulo', 'SP'), data_extracao,
                               self.pos_processar_pt_SP_capital)

        return df

    def pos_processar_pt_SP_capital(self, df):
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'São Paulo'

        for i in range(len(df)):
            cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

            if len(str(cpf_cnpj)) >= 14:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
            else:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = 'CPF/RG'

        return df

    def __get_nome_mes(self, mes):
        nome_mes_atual = calendar.month_name[mes]
        nome_mes_atual = nome_mes_atual[0].upper() + nome_mes_atual[1:]
        return nome_mes_atual
