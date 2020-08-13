import os
from os import path
import time
from datetime import datetime, timedelta
import logging

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait, Select
import pandas as pd

from covidata import config
from covidata.webscraping.selenium.downloader import SeleniumDownloader
from covidata.webscraping.scrappers.SE.consolidacao_SE import consolidar

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


# Define a classe referida como herdeira da class "SeleniumDownloader"
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
        iframe = wait.until(EC.visibility_of_element_located((By.ID, 'dados')))
        self.driver.switch_to.frame(iframe)

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
        iframe = wait.until(EC.visibility_of_element_located((By.ID, 'dados')))
        self.driver.switch_to.frame(iframe)

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
        """
        df_liquidacao.rename(index=str,
                             columns={'DsItemDespesa': 'DsEmpenho', 'Unnamed: 14': 'DsItemDespesa'},
                             inplace=True)
        """
        df_liquidacao.rename(index=str,
                             columns={'DsItemDespesa': 'DsEmpenho'},
                             inplace=True)

        # Acrescenta a coluna "Nome Favorecido" ao objeto pandas DataFrame "df_liquidacao"
        df_liquidacao['Nome Favorecido'] = df_liquidacao['Credor'].apply(lambda x: x.split(' - ')[1])
        # Acrescenta a coluna "CNPJ/CPF Favorecido" ao objeto pandas DataFrame "df_liquidacao"
        df_liquidacao['CNPJ/CPF Favorecido'] = df_liquidacao['Credor'].apply(lambda x: x.split(' - ')[0])

        # Reordena as colunas do objeto pandas DataFrame "df_liquidacao"
        """
        df_liquidacao = df_liquidacao[['Órgão', 'Unidade', 'Data', 'Empenho',
                                       'Liq', 'Nome Favorecido', 'CNPJ/CPF Favorecido',
                                       'Dotação', 'Documento', 'Liquidado', 'Retido',
                                       'DsEmpenho', 'DsItemDespesa']]
        """
        df_liquidacao = df_liquidacao[['Órgão', 'Unidade', 'Data', 'Empenho',
                                       'Liq', 'Nome Favorecido', 'CNPJ/CPF Favorecido',
                                       'Dotação', 'Documento', 'Liquidado', 'Retido',
                                       'DsEmpenho']]

        # Aba "Pagamentos":
        # Entra no iframe de id "dados"
        iframe = wait.until(EC.visibility_of_element_located((By.ID, 'dados')))
        self.driver.switch_to.frame(iframe)

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


def main(df_consolidado):
    data_extracao = datetime.now()
    logger = logging.getLogger('covidata')

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_Aracaju = PortalTransparencia_Aracaju()
    pt_Aracaju.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao, df_consolidado)
    logger.info("--- %s segundos ---" % (time.time() - start_time))

# main()
