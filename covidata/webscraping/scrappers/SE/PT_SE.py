import logging
import os
import time
from datetime import datetime, timedelta
from os import path

import numpy as np
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
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
                            consolidacao.CONTRATADO_DESCRICAO: 'Razão Social Favorecido',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ Favorecido', consolidacao.DESPESA_DESCRICAO: 'Elemento',
                            consolidacao.VALOR_CONTRATO: 'Valor Acumulado (R$)'}

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
        df = consolidar_layout(df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_SE, 'SE', '',
                               data_extracao)
        df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'
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


class PT_Aracaju_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')

        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_Aracaju = PortalTransparencia_Aracaju()
        pt_Aracaju.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_pt_Aracaju(data_extracao), False

    def consolidar_pt_Aracaju(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão',
                            consolidacao.DOCUMENTO_NUMERO: 'Empenho',
                            consolidacao.CONTRATADO_DESCRICAO: 'Nome Favorecido',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ/CPF Favorecido',
                            consolidacao.DESPESA_DESCRICAO: 'DsEmpenho'}

        df_empenhos = pd.read_excel(path.join(config.diretorio_dados, 'SE', 'portal_transparencia',
                                              'Aracaju', 'Dados_Portal_Transparencia_Aracaju.xlsx'),
                                    sheet_name='Empenhos')

        df_liquidacoes = pd.read_excel(path.join(config.diretorio_dados, 'SE', 'portal_transparencia',
                                                 'Aracaju', 'Dados_Portal_Transparencia_Aracaju.xlsx'),
                                       sheet_name='Liquidações')

        df_pagamentos = pd.read_excel(path.join(config.diretorio_dados, 'SE', 'portal_transparencia',
                                                'Aracaju', 'Dados_Portal_Transparencia_Aracaju.xlsx'),
                                      sheet_name='Pagamentos')

        df = self.pre_processar_pt_Aracaju(df_empenhos, df_liquidacoes, df_pagamentos)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Aracaju, 'SE',
                               get_codigo_municipio_por_nome('Aracaju', 'SE'), data_extracao, self.pos_processar)
        return df

    def pos_processar(self, df):
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Aracaju'
        df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'
        return df

    def pre_processar_pt_Aracaju(self, df1, df2, df3):
        # Renomeia colunas do objeto pandas DataFrame "df1"
        df1.rename(index=str,
                   columns={'Data': 'Data Empenho',
                            'Anulado': 'Empenhado Anulado',
                            'Reforçado': 'Empenhado Reforçado'},
                   inplace=True)

        # Renomeia colunas do objeto pandas DataFrame "df2"
        df2.rename(index=str,
                   columns={'Data': 'Data Liquidação',
                            'Liq': 'Liquidação',
                            'Retido': 'Liquidado Retido'},
                   inplace=True)

        # Renomeia colunas do objeto pandas DataFrame "df3"
        df3.rename(index=str,
                   columns={'Data': 'Data Pagamento',
                            'Retido': 'Pagamento Retido'},
                   inplace=True)

        # Faz o merge de "df3" com parte de "df2" por uma coluna tendo por base "df3"
        df = pd.merge(df3, df2[
            ['Data Liquidação', 'Empenho', 'Liquidação', 'Dotação', 'Documento', 'Liquidado', 'Liquidado Retido']],
                      how='left',
                      left_on='Empenho',
                      right_on='Empenho')

        # Faz o merge de "df" com "df1" por uma coluna tendo por base "df"
        df = pd.merge(df, df1[['Data Empenho', 'Empenho', 'Empenhado', 'Empenhado Anulado', 'Empenhado Reforçado']],
                      how='left',
                      left_on='Empenho',
                      right_on='Empenho')

        print(list(df.columns))

        return df


class PortalTransparencia_Aracaju(SeleniumDownloader):

    # Sobrescreve o construtor da class "SeleniumDownloader"
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'SE', 'portal_transparencia', 'Aracaju'),
                         config.url_pt_Aracaju,
                         # browser_option='--start-maximized'
                         )

    # Implementa localmente o método interno e vazio da class "SeleniumDownloader"
    def _executar(self):
        # Cria um objeto da class "WebDriverWait"
        wait = WebDriverWait(self.driver, 45)

        # Entra no iframe de id "dados"
        # iframe = wait.until(EC.visibility_of_element_located((By.ID, 'dados')))
        # self.driver.switch_to.frame(iframe)

        # Aba "Empenhos" (default do iframe):
        # Coloca o campo dropdown dos meses do ano com o valor "Selecione"
        select = Select(self.driver.find_element_by_id('ddlMesEmpenhos'))
        select.select_by_visible_text('Selecione')

        # Seleciona o campo "Data Início" e seta a data de início de busca
        self.driver.find_element_by_id('txtDtInicioEmpenhos').send_keys('01/03/2020')
        # Seleciona o campo "Data Fim" e seta a data de fim de busca como a do dia anterior
        self.driver.find_element_by_id('txtDtFimEmpenhos').send_keys(
            (datetime.today() - timedelta(days=1)).strftime('%d/%m/%Y'))

        # Seleciona o botão "Pesquisar"
        element = wait.until(EC.visibility_of_element_located((By.XPATH,
                                                               '//*[@id="btnFiltrarEmpenhos"]')))
        self.driver.execute_script("arguments[0].click();", element)

        # On hold por 3 segundos
        time.sleep(3)

        # Seleciona o botão "XLSX" salvando o arquivo "xlsx" contendo os dados de empenho
        element = wait.until(EC.visibility_of_element_located((By.XPATH,
                                                               '//*[@id="dataTables-Empenhos_wrapper"]/div[1]/a[1]/span/img')))
        self.driver.execute_script("arguments[0].click();", element)

        self.driver.switch_to.default_content()

        # On hold por 3 segundos
        time.sleep(3)

        # Lê o arquivo "xlsx" de empenhos baixado como um objeto pandas DataFrame selecionando as colunas úteis
        df_empenho = pd.read_excel(path.join(config.diretorio_dados, 'SE', 'portal_transparencia',
                                             'Aracaju', 'Município Online.xlsx'), usecols=list(range(1, 9)) + [11, 12])

        # Substitui o código de órgãos pelo nome
        df_empenho['Órgão'].replace(dict_orgaos_aracaju, inplace=True)

        # Substitui o código de unidades gestoras pelo nome
        df_empenho['Unidade'].replace(dict_unidades_aracaju, inplace=True)

        # Acrescenta a coluna "Nome Favorecido" ao objeto pandas DataFrame "df_empenho"
        df_empenho['Nome Favorecido'] = df_empenho['Credor'].apply(lambda x: x.split(' - ')[1])
        # Acrescenta a coluna "CNPJ/CPF Favorecido" ao objeto pandas DataFrame "df_empenho"
        df_empenho['CNPJ/CPF Favorecido'] = df_empenho['Credor'].apply(lambda x: x.split(' - ')[0])

        # Reordena as colunas do objeto pandas DataFrame "df_empenho"
        df_empenho = df_empenho[['Órgão', 'Unidade', 'Data', 'Empenho', 'Nome Favorecido',
                                 'CNPJ/CPF Favorecido', 'Empenhado', 'Anulado', 'Reforçado',
                                 'DsEmpenho', 'DsItemDespesa']]

        wait = WebDriverWait(self.driver, 45)

        # Aba "Liquidações":
        # Entra no iframe de id "dados"
        # iframe = wait.until(EC.visibility_of_element_located((By.ID, 'dados')))
        # self.driver.switch_to.frame(iframe)

        # Seleciona a aba "Liquidações"
        element = wait.until(EC.visibility_of_element_located((By.ID, 'lnkLiquidacoes')))
        self.driver.execute_script("arguments[0].click();", element)

        # On hold por 3 segundos
        time.sleep(3)

        # Coloca o campo dropdown dos meses do ano com o valor "Selecione"
        select = Select(self.driver.find_element_by_id('ddlMesLiquidacoes'))
        select.select_by_visible_text('Selecione')

        # Seleciona o campo "Data Início" e seta a data de início de busca
        self.driver.find_element_by_id('txtDtInicioLiquidacoes').send_keys('01/03/2020')
        # Seleciona o campo "Data Fim" e seta a data de fim de busca
        self.driver.find_element_by_id('txtDtFimLiquidacoes').send_keys(
            (datetime.today() - timedelta(days=1)).strftime('%d/%m/%Y'))

        # Seleciona o botão "Pesquisar"
        element = wait.until(EC.visibility_of_element_located((By.XPATH,
                                                               '//*[@id="btnFiltrarLiquidacoes"]')))
        self.driver.execute_script("arguments[0].click();", element)

        # On hold por 3 segundos
        time.sleep(3)

        # Seleciona o botão "XLSX" salvando o arquivo "xlsx" contendo os dados de liquidação
        element = wait.until(EC.visibility_of_element_located((By.XPATH,
                                                               '//*[@id="dataTables-Liquidacoes_wrapper"]/div[1]/a[1]/span/img')))
        self.driver.execute_script("arguments[0].click();", element)

        self.driver.switch_to.default_content()

        # On hold por 3 segundos
        time.sleep(3)

        # Lê o arquivo "xlsx" de liquidações baixado como um objeto pandas DataFrame selecionando as colunas úteis
        df_liquidacao = pd.read_excel(path.join(config.diretorio_dados, 'SE', 'portal_transparencia',
                                                'Aracaju', 'Município Online.xlsx'),
                                      usecols=list(range(1, 11)) + [13, 14])

        # Substitui o código de órgãos pelo nome
        df_liquidacao['Órgão'].replace(dict_orgaos_aracaju, inplace=True)

        # Substitui o código de unidades gestoras pelo nome
        df_liquidacao['Unidade'].replace(dict_unidades_aracaju, inplace=True)

        # Renomeia colunas especificadas
        df_liquidacao.rename(index=str,
                             columns={'DsItemDespesa': 'DsEmpenho'},
                             inplace=True)

        # Acrescenta a coluna "Nome Favorecido" ao objeto pandas DataFrame "df_liquidacao"
        df_liquidacao['Nome Favorecido'] = df_liquidacao['Credor'].apply(lambda x: x.split(' - ')[1])
        # Acrescenta a coluna "CNPJ/CPF Favorecido" ao objeto pandas DataFrame "df_liquidacao"
        df_liquidacao['CNPJ/CPF Favorecido'] = df_liquidacao['Credor'].apply(lambda x: x.split(' - ')[0])

        # Reordena as colunas do objeto pandas DataFrame "df_liquidacao"
        df_liquidacao = df_liquidacao[['Órgão', 'Unidade', 'Data', 'Empenho',
                                       'Liq', 'Nome Favorecido', 'CNPJ/CPF Favorecido',
                                       'Dotação', 'Documento', 'Liquidado', 'Retido',
                                       'DsEmpenho']]

        # Aba "Pagamentos":
        # Entra no iframe de id "dados"
        # iframe = wait.until(EC.visibility_of_element_located((By.ID, 'dados')))
        # self.driver.switch_to.frame(iframe)

        # Seleciona a aba "Pagamentos"
        element = wait.until(EC.visibility_of_element_located((By.ID, 'lnkPagamentos')))
        self.driver.execute_script("arguments[0].click();", element)

        # On hold por 3 segundos
        time.sleep(3)

        # Coloca o campo dropdown de meses com o valor "Selecione"
        select = Select(self.driver.find_element_by_id('ddlMesPagamentos'))
        select.select_by_visible_text('Selecione')

        # Seleciona o campo "Data Início" e seta a data de início de busca
        self.driver.find_element_by_id('txtDtInicioPagamentos').send_keys('01/03/2020')
        # Seleciona o campo "Data Fim" e seta a data de fim de busca
        self.driver.find_element_by_id('txtDtFimPagamentos').send_keys(
            (datetime.today() - timedelta(days=1)).strftime('%d/%m/%Y'))

        # Seleciona o botão "Pesquisar"
        element = wait.until(EC.visibility_of_element_located((By.XPATH,
                                                               '//*[@id="btnFiltrarPagamentos"]')))
        self.driver.execute_script("arguments[0].click();", element)

        # On hold por 3 segundos
        time.sleep(3)

        # Seleciona o botão "XLSX" salvando o arquivo "xlsx" contendo os dados de liquidação
        element = wait.until(EC.visibility_of_element_located((By.XPATH,
                                                               '//*[@id="dataTables-Pagamentos_wrapper"]/div[1]/a[1]/span/img')))
        self.driver.execute_script("arguments[0].click();", element)

        self.driver.switch_to.default_content()

        # On hold por 3 segundos
        time.sleep(3)

        # Lê o arquivo "xlsx" de liquidações baixado como um objeto pandas DataFrame selecionando as colunas úteis
        df_pagamento = pd.read_excel(path.join(config.diretorio_dados, 'SE', 'portal_transparencia',
                                               'Aracaju', 'Município Online.xlsx'),
                                     usecols=list(range(1, 10)) + [11, 12])

        # Substitui o código de órgãos pelo nome
        df_pagamento['Órgão'].replace(dict_orgaos_aracaju, inplace=True)

        # Substitui o código de unidades gestoras pelo nome
        df_pagamento['Unidade'].replace(dict_unidades_aracaju, inplace=True)

        # Acrescenta a coluna "Nome Favorecido" ao objeto pandas DataFrame "df_pagamento"
        df_pagamento['Nome Favorecido'] = df_pagamento['Credor'].apply(lambda x: x.split(' - ')[1])
        # Acrescenta a coluna "CNPJ/CPF Favorecido" ao objeto pandas DataFrame "df_pagamento"
        df_pagamento['CNPJ/CPF Favorecido'] = df_pagamento['Credor'].apply(lambda x: x.split(' - ')[0])

        # Reordena as colunas do objeto pandas DataFrame "df_pagamento"
        df_pagamento = df_pagamento[['Órgão', 'Unidade', 'Data', 'Empenho',
                                     'Processo', 'Nome Favorecido', 'CNPJ/CPF Favorecido',
                                     'Pago', 'Retido', 'Nota de Pagamento', 'DsEmpenho',
                                     'DsItemDespesa']]

        # Cria arquivo "xlsx" e aloca file handler de escrita para a variável "writer"
        with pd.ExcelWriter(path.join(config.diretorio_dados, 'SE', 'portal_transparencia',
                                      'Aracaju', 'Dados_Portal_Transparencia_Aracaju.xlsx')) as writer:
            # Salva os dados de empenhos contidos em "df_empenho" na planilha "Empenhos"
            df_empenho.to_excel(writer, sheet_name='Empenhos', index=False)
            # Salva os dados de liquidações contidos em "df_liquidacao" na planilha "Liquidações"
            df_liquidacao.to_excel(writer, sheet_name='Liquidações', index=False)
            # Salva os dados de pagamentos contidos em "df_pagamento" na planilha "Pagamentos"
            df_pagamento.to_excel(writer, sheet_name='Pagamentos', index=False)

        # Deleta o arquivo "xlsx" de nome "Município Online"
        os.unlink(path.join(config.diretorio_dados, 'SE', 'portal_transparencia', 'Aracaju', 'Município Online.xlsx'))


dict_orgaos_aracaju = {12: 'SECRETARIA MUNICIPAL DE GOVERNO',
                       13: 'SECRETARIA MUNICIPAL DA FAZENDA',
                       18: 'SECRETARIA MUNICIPAL DE SAÚDE',
                       19: 'SECRETARIA MUNIC. DA FAMÍLIA E DA ASSISTÊNCIA SOCIAL',
                       20: 'SECRETARIA MUNICIPAL DA COMUNICAÇÃO SOCIAL',
                       21: 'SEC. MUNIC. DO PLANEJAMENTO, ORÇAMENTO E GESTÃO',
                       24: 'SECRETARIA MUNICIPAL DA DEFESA SOCIAL E DA CIDADANIA',
                       28: 'SECRETARIA MUNICIPAL DO MEIO AMBIENTE'}
dict_unidades_aracaju = {12201: 'FUNDAÇÃO CULTURAL CIDADE DE ARACAJU',
                         13101: 'SECRETARIA MUNICIPAL DA FAZENDA',
                         18401: 'FUNDO MUNICIPAL DE SAÚDE',
                         19101: 'SECRETARIA MUNIC. DA FAMÍLIA E DA ASSISTÊNCIA SOCIAL',
                         19401: 'FUNDO MUNICIPAL DE ASSISTÊNCIA SOCIAL',
                         20101: 'SECRETARIA MUNICIPAL DE COMUNICAÇÃO SOCIAL',
                         21101: 'SECRETARIA MUNIC. DO PLANEJ. ORÇAMENTO E GESTÃO',
                         24101: 'SECRETARIA MUNICIPAL DA DEFESA SOCIAL E DA CIDADANIA',
                         28101: 'SECRETARIA MUNICIPAL DO MEIO AMBIENTE'}
