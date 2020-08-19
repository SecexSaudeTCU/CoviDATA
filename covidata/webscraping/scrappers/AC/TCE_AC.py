import logging
import time
from os import path

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from covidata import config
from covidata.municipios.ibge import get_municipios_por_uf
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.persistencia.dao import persistir
from covidata.webscraping.scrappers.scrapper import Scraper


# TODO: Para o portal de transparência, o arquivo CSV disponibilizado não tem os nomes das colunas, não sendo possível
#  portanto entende-lo ao ponto de consolidar as informações.
class TCE_AC_Scraper(Scraper):
    def _extrair(self, url, informacao, indice):
        colunas, linhas_df = self.__extrair_tabela(url, indice)
        df = pd.DataFrame(linhas_df, columns=colunas)
        persistir(df, 'tce', informacao, 'AC')

    def __extrair_tabela(self, url, indice):
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        tabela = soup.find_all('table')[indice]
        linhas = tabela.find_all('tr')
        titulos = linhas[0]
        titulos_colunas = titulos.find_all('td')
        colunas = [titulo_coluna.get_text() for titulo_coluna in titulos_colunas]
        linhas = linhas[1:]
        lista_linhas = []

        for linha in linhas:
            data = linha.find_all("td")
            nova_linha = [data[i].get_text() for i in range(len(colunas))]
            lista_linhas.append(nova_linha)

        return colunas, lista_linhas

    def _definir_municipios(self, df, prefixo='PREFEITURA MUNICIPAL DE\r\n '):
        codigos_municipios = get_municipios_por_uf('AC')
        # Define os municípios
        df[consolidacao.MUNICIPIO_DESCRICAO] = df.apply(
            lambda row: self.__get_nome_municipio(row, prefixo), axis=1)
        df[consolidacao.COD_IBGE_MUNICIPIO] = df.apply(
            lambda row: codigos_municipios.get(row[consolidacao.MUNICIPIO_DESCRICAO].upper(), ''), axis=1)
        df = df.astype({consolidacao.COD_IBGE_MUNICIPIO: str})
        return df

    def __get_nome_municipio(self, row, prefixo='\nPrefeitura Municipal de '):
        string_original = row[consolidacao.CONTRATANTE_DESCRICAO]

        if prefixo in string_original:
            nome_municipio = string_original[len(prefixo):len(string_original)].strip()
            return nome_municipio
        else:
            return ''

    def pos_processar_dispensas(self, df):
        # Elimina a última linha, que só contém um totalizador
        df = df.drop(df.index[-1])
        df[consolidacao.MOD_APLIC_DESCRICAO] = 'Dispensa de Licitação'
        return df


class TCE_AC_ContratosScraper(TCE_AC_Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Tribunal de Contas estadual - contratos...')
        start_time = time.time()
        self._extrair(url=config.url_tce_AC_contratos, informacao='contratos', indice=0)
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        logger = logging.getLogger('covidata')
        logger.info('Consolidando as informações de contratos no layout padronizado...')
        start_time = time.time()
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Entidade', consolidacao.DESPESA_DESCRICAO: ' Objeto ',
                            consolidacao.VALOR_CONTRATO: 'Valor R$', consolidacao.UG_DESCRICAO: 'Entidade',
                            consolidacao.FUNDAMENTO_LEGAL: '\xa0Fundamento Legal\xa0',
                            consolidacao.NUMERO_PROCESSO: 'Nº Processo'}
        colunas_adicionais = ['Cód.\r\n  Dispensa', 'Data Pedido']

        df_original = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'contratos.xls'), header=4)
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_contratos, 'AC', '',
                               data_extracao)
        # Elimina as duas últimas linhas
        df.drop(df.tail(2).index, inplace=True)
        logger.info("--- %s segundos ---" % (time.time() - start_time))
        return df, False


class TCE_AC_DespesasScraper(TCE_AC_Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Tribunal de Contas estadual - despesas...')
        start_time = time.time()
        self._extrair(url=config.url_tce_AC_despesas, informacao='despesas', indice=0)
        self._extrair(url=config.url_tce_AC_despesas, informacao='dispensas', indice=1)
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        logger = logging.getLogger('covidata')
        logger.info('Consolidando as informações de despesas no layout padronizado...')
        start_time = time.time()
        despesas = self.__consolidar_despesas(data_extracao)
        dispensas = self.__consolidar_dispensas(data_extracao)
        despesas = despesas.append(dispensas, ignore_index=True, sort=False)
        logger.info("--- %s segundos ---" % (time.time() - start_time))
        return despesas, False

    def __consolidar_despesas(self, data_extracao):
        dicionario_dados = {consolidacao.DOCUMENTO_NUMERO: '\nNUMEROEMPENHO\n',
                            consolidacao.CONTRATADO_DESCRICAO: '\nRazão Social\n',
                            consolidacao.CONTRATADO_CNPJ: '\nCPF/CNPJ\n',
                            consolidacao.DOCUMENTO_DATA: '\nData do Empenho\n',
                            consolidacao.FONTE_RECURSOS_COD: '\nFonte de Recurso\n',
                            consolidacao.VALOR_EMPENHADO: '\nValor Empenhado\r\n  ($)\n',
                            # TODO: Dúvida: Podemos assumir para VALOR_R$ o valor empenhado, quando essa informação não estiver disponível?
                            consolidacao.VALOR_CONTRATO: '\nValor Empenhado\r\n  ($)\n'}
        # TODO: Sugerir uma nova coluna 'TIPO_FONTE'
        # TODO: Nem sempre esta informação está presente
        df_original = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'despesas.xls'), header=4)
        df = consolidar_layout(['\nTipo de Credor\n'], df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_despesas, 'AC', '',
                               data_extracao,
                               self.pos_processar_despesas)
        df.fillna('')
        return df

    def __consolidar_dispensas(self, data_extracao):
        dicionario_dados = {consolidacao.DESPESA_DESCRICAO: '\nObjeto\n',
                            consolidacao.VALOR_CONTRATO: '\nValor\r\n  R$\n',
                            consolidacao.CONTRATANTE_DESCRICAO: '\nEnte\n', consolidacao.UG_DESCRICAO: '\nEnte\n',
                            consolidacao.CONTRATADO_DESCRICAO: '\nFornecedor\n',
                            consolidacao.NUMERO_PROCESSO: '\nNúmero\r\n  Processo\n'}
        colunas_adicionais = ['\nData da Alimentação\n']
        df_original = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'dispensas.xls'), header=4)
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_despesas, 'AC', '',
                               data_extracao, self.pos_processar_dispensas)
        return df

    def pos_processar_despesas(self, df):
        # Elimina a última linha, que só contém um totalizador
        df = df.drop(df.index[-1])

        df = df.astype({consolidacao.CONTRATADO_CNPJ: np.uint64, consolidacao.DOCUMENTO_NUMERO: np.uint64})
        df = df.astype({consolidacao.CONTRATADO_CNPJ: str, consolidacao.DOCUMENTO_NUMERO: str})

        df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'
        df.fillna('')

        for i in range(0, len(df)):
            cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

            if len(cpf_cnpj) == 11:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
            elif len(cpf_cnpj) > 11:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

        return df


class TCE_AC_ContratosMunicipiosScraper(TCE_AC_Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Tribunal de Contas estadual - contratos municipais...')
        start_time = time.time()
        self._extrair(url=config.url_tce_AC_contratos_municipios, informacao='contratos_municipios', indice=0)
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        logger = logging.getLogger('covidata')
        logger.info('Consolidando as informações de contratos municipais no layout padronizado...')
        start_time = time.time()
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Entidade', consolidacao.DESPESA_DESCRICAO: ' Objeto ',
                            consolidacao.VALOR_CONTRATO: 'Valor R$', consolidacao.UG_DESCRICAO: 'Entidade',
                            consolidacao.FUNDAMENTO_LEGAL: '\xa0Fundamento Legal\xa0',
                            consolidacao.NUMERO_PROCESSO: 'Nº Processo'}
        colunas_adicionais = ['Cód.\r\n  Dispensa', 'Data Pedido']

        df_original = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'contratos_municipios.xls'),
                                    header=4)
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_contratos_municipios, 'AC', '',
                               data_extracao, self.pos_processar_contratos_municipios)
        logger.info("--- %s segundos ---" % (time.time() - start_time))
        return df, False

    def pos_processar_contratos_municipios(self, df):
        # Elimina as três últimas linhas
        df.drop(df.tail(2).index, inplace=True)
        df = self._definir_municipios(df)
        return df


class TCE_AC_DespesasMunicipiosScraper(TCE_AC_Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Tribunal de Contas estadual - despesas municipais...')
        start_time = time.time()
        self._extrair(url=config.url_tce_AC_despesas_municipios, informacao='despesas_municipios', indice=0)
        self._extrair(url=config.url_tce_AC_despesas_municipios, informacao='dispensas_municipios', indice=1)
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        logger = logging.getLogger('covidata')
        logger.info('Consolidando as informações de despesas municipais no layout padronizado...')
        start_time = time.time()
        despesas_municipios = self.__consolidar_despesas_municipios(data_extracao)
        dispensas_municipios = self.__consolidar_dispensas_municipios(data_extracao)
        despesas_municipios = despesas_municipios.append(dispensas_municipios, ignore_index=True, sort=False)
        logger.info("--- %s segundos ---" % (time.time() - start_time))
        return despesas_municipios, False

    def __consolidar_despesas_municipios(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: '\nPREFEITURAS MUNICIPAIS NO ESTADO DO ACRE\n',
                            consolidacao.UG_DESCRICAO: '\nPREFEITURAS MUNICIPAIS NO ESTADO DO ACRE\n',
                            consolidacao.CONTRATADO_CNPJ: '\nCNPJ/CPF\n',
                            consolidacao.VALOR_CONTRATO: '\n VALOR CONTRATADO R$\n'}
        colunas_adicionais = ['\nCONTRATOS/OBSERVAÇÕES\n']

        df_original = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'despesas_municipios.xls'), header=4)
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_despesas_municipios, 'AC', '',
                               data_extracao, self.pos_processar_despesas_municipio)
        return df

    def __consolidar_dispensas_municipios(self, data_extracao):
        dicionario_dados = {consolidacao.DESPESA_DESCRICAO: '\nObjeto\n',
                            consolidacao.VALOR_CONTRATO: '\nValor\r\n  R$\n',
                            consolidacao.CONTRATANTE_DESCRICAO: '\nEnte\n', consolidacao.UG_DESCRICAO: '\nEnte\n',
                            consolidacao.CONTRATADO_DESCRICAO: '\nFornecedor\n',
                            consolidacao.NUMERO_PROCESSO: '\nNúmero\r\n  Processo\n'}
        colunas_adicionais = ['\nData\r\n  da Alimentação\n']
        df_original = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'dispensas_municipios.xls'),
                                    header=4)
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_despesas_municipios, 'AC', '',
                               data_extracao, self.pos_processar_dispensas_municipios)
        return df

    def pos_processar_despesas_municipio(self, df):
        # Elimina a última linha, que só contém um totalizador
        df = df.drop(df.index[-1])
        df.fillna('')

        for i in range(0, len(df)):
            cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ].strip()

            if len(cpf_cnpj) == 14:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
            elif len(cpf_cnpj) > 14:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

        df = self._definir_municipios(df, prefixo='\nPrefeitura Municipal de ')

        return df

    def pos_processar_dispensas_municipios(self, df):
        df = self.pos_processar_dispensas(df)
        df = self._definir_municipios(df, prefixo='\nPREFEITURA MUNICIPAL DE ')
        df = df.rename(columns={'DATA\r\n  DA ALIMENTAÇÃO': 'DATA DA ALIMENTAÇÃO'})
        df[consolidacao.MOD_APLIC_DESCRICAO] = 'Dispensa de Licitação'
        return df
