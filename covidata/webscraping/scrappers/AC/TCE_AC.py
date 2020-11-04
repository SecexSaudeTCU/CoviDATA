from os import path

import logging
import pandas as pd
import requests
import time
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
                            consolidacao.CONTRATADO_CNPJ: '\nCNPJ/CPF\n',
                            consolidacao.CONTRATADO_DESCRICAO: '\nCONTRATOS/OBSERVAÇÕES\n',
                            consolidacao.VALOR_CONTRATO: '\n VALOR CONTRATADO R$\n'}

        df_original = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'despesas_municipios.xls'), header=4)
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_despesas_municipios, 'AC', '',
                               data_extracao, self.pos_processar_despesas_municipio)
        return df

    def __consolidar_dispensas_municipios(self, data_extracao):
        dicionario_dados = {consolidacao.DESPESA_DESCRICAO: '\nObjeto\n',
                            consolidacao.VALOR_CONTRATO: '\nValor\r\n  R$\n',
                            consolidacao.CONTRATANTE_DESCRICAO: '\nEnte\n',
                            consolidacao.CONTRATADO_DESCRICAO: '\nFornecedor\n'}
        df_original = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'dispensas_municipios.xls'),
                                    header=4)
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_despesas_municipios, 'AC', '',
                               data_extracao, self.pos_processar_dispensas_municipios)
        return df

    def pos_processar_despesas_municipio(self, df):
        # Elimina a última linha, que só contém um totalizador
        df = df.drop(df.index[-1])
        df.fillna('')
        df = self._definir_municipios(df, prefixo='\nPrefeitura Municipal de ')

        for i in range(0, len(df)):
            obs = df.loc[i, consolidacao.CONTRATADO_DESCRICAO]
            cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

            if cnpj and cnpj.strip() != '':
                if 'Contrato' in obs:
                    df.loc[i, consolidacao.CONTRATADO_DESCRICAO] = obs[obs.find('-') + 1:]
                else:
                    df.loc[i, consolidacao.CONTRATADO_DESCRICAO] = obs

        return df

    def pos_processar_dispensas_municipios(self, df):
        # Elimina a última linha, que só contém um totalizador
        df = df.drop(df.index[-1])
        df = self._definir_municipios(df, prefixo='\nPREFEITURA MUNICIPAL DE ')
        df = df.rename(columns={'DATA\r\n  DA ALIMENTAÇÃO': 'DATA DA ALIMENTAÇÃO'})
        return df
