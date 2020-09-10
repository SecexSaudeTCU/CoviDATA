import logging
import os
import time
from datetime import datetime
from os import path

import numpy as np
import pandas as pd

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader

dict_meses = {0: 'Janeiro',
              1: 'Fevereiro',
              2: 'Março',
              3: 'Abril',
              4: 'Maio',
              5: 'Junho',
              6: 'Julho',
              7: 'Agosto',
              8: 'Setembro',
              9: 'Outubro',
              10: 'Novembro',
              11: 'Dezembro'}


class PT_SE_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()
        pt_SE = PortalTransparencia_SE()
        pt_SE.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_pt_SE(data_extracao), False

    def consolidar_pt_SE(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Unidade',
                            consolidacao.DOCUMENTO_NUMERO: 'Nº do empenho',
                            consolidacao.PROGRAMA_DESCRICAO: 'Programa',
                            consolidacao.ELEMENTO_DESPESA_DESCRICAO: 'Elemento',
                            consolidacao.CONTRATADO_DESCRICAO: 'Razão Social Favorecido',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ Favorecido',
                            consolidacao.VALOR_EMPENHADO: 'Empenhado',
                            consolidacao.VALOR_LIQUIDADO: 'Liquidado'}

        # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
        colunas_adicionais = ['Mês Empenho', 'Empenhado Original', 'Empenhado Reforço', 'Mês Liquidação']

        # Lê o arquivo "xlsx" de nome "Dados_Portal_Transparencia_Sergipe" de contratos baixado como um objeto pandas DataFrame

        df_empenhos = pd.read_excel(path.join(config.diretorio_dados, 'SE', 'portal_transparencia',
                                              'Sergipe', 'Dados_Portal_Transparencia_Sergipe.xlsx'),
                                    sheet_name='Empenhos')

        df_liquidacoes = pd.read_excel(path.join(config.diretorio_dados, 'SE', 'portal_transparencia',
                                                 'Sergipe', 'Dados_Portal_Transparencia_Sergipe.xlsx'),
                                       sheet_name='Liquidações')

        # Chama a função "pre_processar_pt_SE" definida neste módulo
        df = self.pre_processar_pt_SE(df_empenhos, df_liquidacoes)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(colunas_adicionais, df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_SE, 'SE', '',
                               data_extracao, self.pos_processar_pt)
        return df

    def pre_processar_pt_SE(self, df1, df2):
        # Renomeia colunas do objeto pandas DataFrame "df1"
        df1.rename(index=str,
                   columns={'Mês': 'Mês Empenho',
                            'Valor Original (R$)': 'Empenhado Original',
                            'Valor Reforço (R$)': 'Empenhado Reforço',
                            'Valor Total (R$)': 'Empenhado'},
                   inplace=True)

        # Renomeia colunas do objeto pandas DataFrame "df2"
        df2.rename(index=str,
                   columns={'Mês': 'Mês Liquidação',
                            'Valor do Mês (R$)': 'Liquidado'},
                   inplace=True)

        # Faz o merge de "df1" com "df2" por uma coluna tendo por base "df2"
        df = pd.merge(df1, df2[['Nº do empenho', 'Mês Liquidação', 'Liquidado']],
                      how='right',
                      left_on='Nº do empenho',
                      right_on='Nº do empenho')

        return df

    def pos_processar_pt(self, df):

        for i in range(len(df)):
            cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

            if len(str(cpf_cnpj)) >= 14:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
            else:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = 'CPF/RG'

        return df


# Define a classe referida como herdeira da class "SeleniumDownloader"
class PortalTransparencia_SE(SeleniumDownloader):

    # Sobrescreve o construtor da class "SeleniumDownloader"
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'SE', 'portal_transparencia', 'Sergipe'),
                         config.url_pt_SE)

    # Implementa localmente o método interno e vazio da class "SeleniumDownloader"
    def _executar(self):
        # Inicializa objeto pandas DataFrame dos empenhos
        df_empenho = pd.DataFrame(columns=['Unidade', 'Nº do empenho', 'Programa', 'Elemento',
                                           'Razão Social Favorecido', 'CNPJ Favorecido', 'Mês',
                                           'Valor Original (R$)', 'Valor Reforço (R$)',
                                           'Valor Total (R$)'])

        # Inicializa objeto pandas DataFrame das liquidações
        df_liquidacao = pd.DataFrame(columns=['Unidade', 'Nº do empenho', 'Programa', 'Elemento',
                                              'Razão Social Favorecido', 'CNPJ Favorecido', 'Mês',
                                              'Valor do Mês (R$)'])

        # Inicializa objeto pandas DataFrame dos pagamentos
        df_pagamento = pd.DataFrame(columns=['Unidade', 'Programa', 'Elemento', 'Razão Social Favorecido',
                                             'CNPJ Favorecido', 'Mês', 'Valor do Mês (R$)'])

        # Itera sobre os meses do ano 2020 ("0 = Janeiro",..., "X = Mês atual")
        for month in np.arange(0, int(datetime.today().strftime('%m'))):
            # Seleciona o mês "month"
            self.driver.find_element_by_id("frmPrincipal:mes").click()
            self.driver.find_element_by_xpath(f'//*[@id="frmPrincipal:mes_{month}"]').click()

            # Seleciona o botão em forma de lupa inclinada a 135 graus
            self.driver.find_element_by_id("frmPrincipal:botaoPesquisar").click()

            # Seleciona a aba "Empenhos"
            self.driver.find_element_by_xpath('//*[@id="frmPrincipal:abas"]/ul/li[1]/a').click()
            # Seleciona o link do arquivo "csv" respectivo
            # self.driver.find_element_by_xpath('//*[@id="frmPrincipal:abas:j_idt234"]/span[2]').click()
            self.driver.find_element_by_xpath('//*[@id="frmPrincipal:abas:j_idt232"]/span[2]').click()

            # On hold por 3 segundos
            time.sleep(3)

            # Lê o arquivo "csv" de empenhos baixado para o mês "month"
            df_empenho_mes = pd.read_csv(path.join(config.diretorio_dados, 'SE',
                                                   'portal_transparencia', 'Sergipe', 'Elemento_de_Despesa.csv'))

            # Acrescenta a coluna "Razão Social Favorecido" ao objeto pandas DataFrame "df_empenho_mes"
            df_empenho_mes['Razão Social Favorecido'] = df_empenho_mes['Nome do Favorecido']
            # Acrescenta a coluna "CNPJ Favorecido" ao objeto pandas DataFrame "df_empenho_mes"
            # df_empenho_mes['CNPJ Favorecido'] = df_empenho_mes['Código do Favorecido'].apply(lambda x: x.split('-')[0])
            df_empenho_mes['CNPJ Favorecido'] = df_empenho_mes['Código do Favorecido']
            # Acrescenta a coluna "Mês" ao objeto pandas DataFrame "df_empenho_mes"
            df_empenho_mes['Mês'] = dict_meses[month]

            # Reordena as colunas do objeto pandas DataFrame "df_empenho_mes"
            df_empenho_mes = df_empenho_mes[['Unidade', 'Nº do empenho', 'Programa', 'Elemento',
                                             'Razão Social Favorecido', 'CNPJ Favorecido', 'Mês',
                                             'Valor Original (R$)', 'Valor Reforço (R$)',
                                             'Valor Anulado (R$)', 'Valor Acumulado (R$)']]

            # Concatena "df_empenho_mes" a "df_empenho"
            df_empenho = pd.concat([df_empenho, df_empenho_mes])

            # Seleciona a aba "Liquidações"
            self.driver.find_element_by_xpath('//*[@id="frmPrincipal:abas"]/ul/li[2]/a').click()
            # Seleciona o link do arquivo "csv" respectivo
            # self.driver.find_element_by_xpath('//*[@id="frmPrincipal:abas:j_idt257"]/span[2]').click()
            self.driver.find_element_by_xpath('//*[@id="frmPrincipal:abas:j_idt260"]/span[2]').click()

            # On hold por 3 segundos
            time.sleep(3)

            # Lê o arquivo "csv" de liquidações baixado para o mês "month"
            df_liquidacao_mes = pd.read_csv(path.join(config.diretorio_dados, 'SE',
                                                      'portal_transparencia', 'Sergipe', 'Elemento_de_Despesa.csv'))

            # Acrescenta a coluna "Razão Social Favorecido" ao objeto pandas DataFrame "df_liquidacao_mes"
            df_liquidacao_mes['Razão Social Favorecido'] = df_liquidacao_mes['Nome do Favorecido']
            # Acrescenta a coluna "CNPJ Favorecido" ao objeto pandas DataFrame "df_liquidacao_mes"
            df_liquidacao_mes['CNPJ Favorecido'] = df_liquidacao_mes['Código do Favorecido'].apply(
                lambda x: str(x).split('-')[0])
            # Acrescenta a coluna "Mês" ao objeto pandas DataFrame "df_liquidacao_mes"
            df_liquidacao_mes['Mês'] = dict_meses[month]

            # Reordena as colunas do objeto pandas DataFrame "df_liquidacao_mes"
            df_liquidacao_mes = df_liquidacao_mes[['Unidade', 'Nº do empenho', 'Programa', 'Elemento',
                                                   'Razão Social Favorecido', 'CNPJ Favorecido', 'Mês',
                                                   'Valor do Mês (R$)']]

            # Concatena "df_liquidacao_mes" a "df_liquidacao"
            df_liquidacao = pd.concat([df_liquidacao, df_liquidacao_mes])

            # Seleciona a aba "Pagamentos"
            self.driver.find_element_by_xpath('//*[@id="frmPrincipal:abas"]/ul/li[3]/a').click()
            # Seleciona o link do arquivo "csv" respectivo
            # self.driver.find_element_by_xpath('//*[@id="frmPrincipal:abas:j_idt276"]/span[2]').click()
            self.driver.find_element_by_xpath('//*[@id="frmPrincipal:abas:j_idt281"]/span[2]').click()

            # On hold por 3 segundos
            time.sleep(3)

            # Lê o arquivo "csv" de pagamentos baixado para o mês "month"
            df_pagamento_mes = pd.read_csv(path.join(config.diretorio_dados, 'SE',
                                                     'portal_transparencia', 'Sergipe', 'Elemento_de_Despesa.csv'))

            # Acrescenta a coluna "Razão Social Favorecido" ao objeto pandas DataFrame "df_pagamento_mes"
            df_pagamento_mes['Razão Social Favorecido'] = df_pagamento_mes['Nome do Favorecido']
            # Acrescenta a coluna "CNPJ Favorecido" ao objeto pandas DataFrame "df_pagamento_mes"
            df_pagamento_mes['CNPJ Favorecido'] = df_pagamento_mes['Código do Favorecido']
            # Acrescenta a coluna "Mês" ao objeto pandas DataFrame "df_pagamento_mes"
            df_pagamento_mes['Mês'] = dict_meses[month]

            # Reordena as colunas do objeto pandas DataFrame "df_pagamento_mes"
            df_pagamento_mes = df_pagamento_mes[['Unidade', 'Programa', 'Elemento', 'Razão Social Favorecido',
                                                 'CNPJ Favorecido', 'Mês', 'Valor do Mês (R$)']]

            # Concatena "df_pagamento_mes" a "df_pagamento"
            df_pagamento = pd.concat([df_pagamento, df_pagamento_mes])

            # Cria arquivo "xlsx" e aloca file handler de escrita para a variável "writer"
            with pd.ExcelWriter(path.join(config.diretorio_dados, 'SE',
                                          'portal_transparencia', 'Sergipe',
                                          'Dados_Portal_Transparencia_Sergipe.xlsx')) as writer:
                # Salva os dados de empenhos contidos em "df_empenho" na planilha "Empenhos"
                df_empenho.to_excel(writer, sheet_name='Empenhos', index=False)
                # Salva os dados de liquidações contidos em "df_liquidacao" na planilha "Liquidações"
                df_liquidacao.to_excel(writer, sheet_name='Liquidações', index=False)
                # Salva os dados de pagamentos contidos em "df_pagamento" na planilha "Pagamentos"
                df_pagamento.to_excel(writer, sheet_name='Pagamentos', index=False)

        # Deleta o arquivo remanescente "csv" de nome "Elemento_de_Despesa"
        os.unlink(path.join(config.diretorio_dados, 'SE',
                            'portal_transparencia', 'Sergipe', 'Elemento_de_Despesa.csv'))
