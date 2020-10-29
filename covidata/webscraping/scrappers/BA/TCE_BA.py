import logging

import time
from os import path
import pandas as pd
from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class TCE_BA_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Tribunal de Contas estadual...')
        start_time = time.time()
        tce_BA = TCE_BA()
        tce_BA.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_tce(data_extracao), False

    def consolidar_tce(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.DESPESA_DESCRICAO: 'Item de Exibição',
                            consolidacao.CONTRATANTE_DESCRICAO: 'Órgão',
                            consolidacao.CONTRATADO_DESCRICAO: 'Credor',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ Fornecedor'}

        # Lê o arquivo "xlsx" de aquisições nacionais baixado como um objeto pandas DataFrame
        df_original = pd.read_excel(path.join(config.diretorio_dados, 'BA', 'tce',
                                              'MiranteRelDadosAbertosPainelPublicoCovid19Nacional.xlsx'),
                                    usecols=[0, 1] + list(range(3, 16)))

        # Chama a função "pre_processar_tce" definida neste módulo
        df = self.pre_processar_tce(df_original)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_BA, 'BA', '',
                               data_extracao)
        return df

    def pre_processar_tce(self, df):
        # Seleciona apenas os dados da Bahia
        df = df[df['UF'] == '(BA)']

        # Reseta o index
        df.reset_index(drop=True, inplace=True)

        # Junta as colunas "Órgão comprador" e "Text" em uma única
        df['Órgão'] = df[['Órgão comprador', 'Text']].apply(lambda x: ' - '.join(str(x)), axis=1)

        # Eliminhas as colunas especificadas
        df.drop(['UF', 'Órgão comprador', 'Text'], axis=1, inplace=True)

        # Renomeia as colunas especificadas
        df.rename(columns={'Unid.': 'Unidade Item',
                           'Qtde': 'Quantidade Item',
                           'Valor Unit.': 'PU Item',
                           'Valor da compra': 'Preço Item',
                           'Data': 'Data Compra',
                           'Referencia': 'Origem Dados',
                           'Qtd de Compras': 'Quantidade Compras'},
                  inplace=True)

        return df


class TCE_BA(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'BA', 'tce'),
                         config.url_tce_BA,
                         # browser_option='--start-maximized'
                         )

    def _executar(self):
        # On hold por 5 segundos
        time.sleep(5)

        # Seleciona o link referido pelo nome "Pressione..."
        self.driver.find_element_by_link_text(
            'Pressione aqui para baixar os dados do painel em formato de planilha eletrônica').click()

        # On hold por 5 segundos
        time.sleep(5)
