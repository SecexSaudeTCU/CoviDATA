from os import path

import json
import logging
import numpy as np
import os
import pandas as pd
import requests
import time
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_PB_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()
        pt_PB = PortalTransparencia_PB()
        pt_PB.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_pt_PB(data_extracao), False

    def consolidar_pt_PB(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {
            consolidacao.CONTRATADO_CNPJ: 'CNPJ/CPF Favorecido',
            consolidacao.CONTRATADO_DESCRICAO: 'Nome Favorecido',
            consolidacao.DESPESA_DESCRICAO: 'Objetivo',
            consolidacao.VALOR_CONTRATO: 'Valor', consolidacao.CONTRATANTE_DESCRICAO: 'Órgão'}

        # Lê o arquivo "xlsx" de contratos baixado como um objeto pandas DataFrame selecionando as linhas e colunas úteis
        df_original = pd.read_excel(path.join(config.diretorio_dados, 'PB', 'portal_transparencia', 'Paraiba',
                                              'ListaContratos.xlsx'),
                                    usecols=[0, 3, 4, 5, 6, 7, 9, 11],
                                    skiprows=list(range(4)))

        # Chama a função "pre_processar_pt_PB" definida neste módulo
        df = self.pre_processar_pt_PB(df_original)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_PB, 'PB', '',
                               data_extracao)

        return df

    def pre_processar_pt_PB(self, df):
        # Remove a última linha do objeto pandas DataFrame "df"
        df.drop(df.tail(1).index, inplace=True)

        # Converte as colunas de datas para objetos string
        for col in np.array(['Início', 'Final']):
            df[col] = df[col].apply(lambda x: x.strftime('%d/%m/%Y'))

        # Acrescenta a coluna "Nome Favorecido" ao objeto pandas DataFrame "df"
        df['Nome Favorecido'] = df['Contratado'].apply(lambda x: x.split(' - ')[1])
        # Acrescenta a coluna "CNPJ/CPF Favorecido" ao objeto pandas DataFrame "df"
        df['CNPJ/CPF Favorecido'] = df['Contratado'].apply(lambda x: x.split(' - ')[0])

        # Reordena as colunas do objeto pandas DataFrame "df"
        df = df[['Contrato', 'Nº Licitação', 'Início', 'Final', 'Órgão',
                 'Nome Favorecido', 'CNPJ/CPF Favorecido', 'Objetivo',
                 'Valor']]

        return df


class PortalTransparencia_PB(SeleniumDownloader):

    # Sobrescreve o construtor da class "SeleniumDownloader"
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'PB', 'portal_transparencia', 'Paraiba'),
                         config.url_pt_PB,
                         browser_option='--start-maximized'
                         )

    # Implementa localmente o método interno e vazio da class "SeleniumDownloader"
    def _executar(self):
        # Cria um objeto da class "WebDriverWait"
        wait = WebDriverWait(self.driver, 45)

        # Seleciona o botão "Exibir Relatório"
        element = wait.until(EC.visibility_of_element_located((By.XPATH,
                                                               '//*[@id="RPTRender_ctl08_ctl00"]')))
        self.driver.execute_script("arguments[0].click();", element)

        # On hold por 8 segundos
        time.sleep(8)

        # Seleciona o dropdown menu em forma de "disquete"
        self.driver.find_element_by_id('RPTRender_ctl09_ctl04_ctl00_ButtonImg').click()

        # Seleciona a opção "Excel" do dropdown menu salvando o arquivo "xlsx" contendo os dados de contratos COVID-19
        self.driver.find_element_by_xpath('//*[@id="RPTRender_ctl09_ctl04_ctl00_Menu"]/div[2]/a').click()


class PT_JoaoPessoa_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        self.pt_JoaoPessoa()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def pt_JoaoPessoa(self):
        # Realiza o web scraping da tabela principal do portal da transparência de João Pessoa
        json_content = self.__baixa_arquivo()

        # Realiza o parsing do arquivo JSON para extração dos dados da tabela e retorna um objeto pandas DataFrame
        df_despesas = self.__esquadrinha_json(json_content)

        # Cria diretório do portal da transparência de João Pessoa
        diretorio_jp = os.path.join(config.diretorio_dados, 'PB', 'portal_transparencia', 'JoaoPessoa')
        if not os.path.exists(diretorio_jp): os.makedirs(diretorio_jp)

        # Salva os dados de despesas contidos em "df_despesas" num arquivo "xlsx"
        df_despesas.to_excel(os.path.join(diretorio_jp, 'Dados_Portal_Transparencia_JoaoPessoa.xlsx'), index=False)

    def __baixa_arquivo(self):
        """
        Realiza o web scraping do conteúdo da tabela principal do portal e retorna o conteúdo no formato JSON.
        """

        # URL utilizada para obtenção de dados mostrados no painel do PT João Pessoa
        url = 'https://transparencia.joaopessoa.pb.gov.br:8080/despesas-detalhamento'

        # Cabeçalhos da requisição POST
        headers = {'Host': 'transparencia.joaopessoa.pb.gov.br:8080',
                   'Connection': 'keep-alive',
                   'Content-Length': '12',
                   'Accept': 'application/json, text/plain, */*',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
                   'Content-Type': 'application/json',
                   'Origin': 'https://transparencia.joaopessoa.pb.gov.br',
                   'Sec-Fetch-Site': 'same-site',
                   'Sec-Fetch-Mode': 'cors',
                   'Sec-Fetch-Dest': 'empty',
                   'Referer': 'https://transparencia.joaopessoa.pb.gov.br/',
                   'Accept-Encoding': 'gzip, deflate, br',
                   'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7'}

        # Payload da requisição POST
        payload = '{"ano":2020}'

        # Realizar a requisição POST para obtenção dos dados da tabela principal da seção  "Compras Diretas SIGA"
        r = requests.post(url, headers=headers, data=payload.encode('utf-8'))

        # Obtém os dados em formato JSON com o conteúdo da tabela
        d = json.loads(r.content)

        return d

    def __esquadrinha_json(self, json):
        """
        Realiza o parsing do arquivo JSON e retorna o conteúdo da tabela em Data Frame.
        """

        # Inicializa objetos list para armazenar dados contidos no "dicionário" "json"
        col_nume_empenho = []
        col_data_empenho = []
        col_unidade_orcamentaria = []
        col_favorecido = []
        col_valor_empenhado = []
        col_valor_liquidado = []
        col_valor_pago = []
        col_saldo_pagar = []

        # Aloca os valores das referidas chaves do "dicionário" "json" aos respectivos objetos list
        for i in range(len(json)):
            col_nume_empenho.append(json[i]['nume_empenho'])
            col_data_empenho.append(json[i]['data_empenho'])
            col_unidade_orcamentaria.append(json[i]['unidade_orcamentaria'])
            col_favorecido.append(json[i]['favorecido'])
            col_valor_empenhado.append(json[i]['valor_empenhado'])
            col_valor_liquidado.append(json[i]['valor_liquidado'])
            col_valor_pago.append(json[i]['valor_pago'])
            col_saldo_pagar.append(json[i]['saldo_pagar'])

        # Armazena os dados dos objetos list como um objeto pandas DataFrame
        df = pd.DataFrame(list(zip(col_nume_empenho, col_data_empenho, col_unidade_orcamentaria, col_favorecido,
                                   col_valor_empenhado, col_valor_liquidado, col_valor_pago, col_saldo_pagar)),
                          columns=['Empenho', 'Data Empenho', 'Unidade', 'Nome Favorecido',
                                   'Valor Empenhado', 'Valor Liquidado', 'Valor Pago', 'Saldo a Pagar'])

        # Slice da coluna de string "Data Empenho" apenas os caracteres de data (10 primeiros), em seguida...
        # os converte para datatime, em seguida para date apenas e, por fim, para string no formato "dd/mm/aaaa"
        df['Data Empenho'] = \
            df['Data Empenho'].apply(lambda x: datetime.strptime(x[:10], '%Y-%m-%d').date().strftime('%d/%m/%Y'))

        return df

    def consolidar(self, data_extracao):
        return self.consolidar_pt_JoaoPessoa(data_extracao), False

    def consolidar_pt_JoaoPessoa(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {
            consolidacao.CONTRATADO_DESCRICAO: 'Nome Favorecido',
            consolidacao.DOCUMENTO_NUMERO: 'Empenho',
            consolidacao.DOCUMENTO_DATA: 'Data Empenho',
            consolidacao.CONTRATANTE_DESCRICAO: 'Unidade'
        }

        # Lê o arquivo "xlsx" de contratos baixado como um objeto pandas DataFrame selecionando as linhas e colunas úteis
        df_original = pd.read_excel(path.join(config.diretorio_dados, 'PB', 'portal_transparencia', 'JoaoPessoa',
                                              'Dados_Portal_Transparencia_JoaoPessoa.xlsx'))

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_JoaoPessoa, 'PB',
                               get_codigo_municipio_por_nome('João Pessoa', 'PB'), data_extracao, self.pos_processar)

        return df

    def pos_processar(self, df):
        df[consolidacao.MUNICIPIO_DESCRICAO] = 'João Pessoa'
        df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'
        return df
