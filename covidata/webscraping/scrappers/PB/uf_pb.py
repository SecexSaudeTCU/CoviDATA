import os
from os import path
import time
from datetime import datetime
import logging
import requests
import json

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import numpy as np
import pandas as pd

from covidata import config
from covidata.webscraping.selenium.downloader import SeleniumDownloader
from covidata.webscraping.scrappers.PB.consolidacao_PB import consolidar


# Define a classe referida como herdeira da class "SeleniumDownloader"
class PortalTransparencia_PB(SeleniumDownloader):

    # Sobrescreve o construtor da class "SeleniumDownloader"
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'PB', 'portal_transparencia', 'Paraiba'),
                         config.url_pt_PB,
                         browser_option='--start-maximized')

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


def pt_JoaoPessoa():

    # Realiza o web scraping da tabela principal do portal da transparência de João Pessoa
    json_content = __baixa_arquivo()

    # Realiza o "parsing" do arquivo JSON para extração dos dados da tabela e retorna um objeto pandas DataFrame
    df_despesas = __esquadrinha_json(json_content)

    # Cria diretório do portal da transparência de João Pessoa
    diretorio_jp = os.path.join(path.join(config.diretorio_dados, 'PB', 'portal_transparencia', 'JoaoPessoa'))
    if not os.path.exists(diretorio_jp): os.makedirs(diretorio_jp)

    # Salva os dados de despesas contidos em "df_despesas" num arquivo "xlsx"
    df_despesas.to_excel(os.path.join(diretorio_jp, 'Dados_Portal_Transparencia_JoaoPessoa.xlsx'), index=False)


def __baixa_arquivo():
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


def __esquadrinha_json(json):
    """
    Realiza o "parsing" (esquadrinha) do arquivo JSON e retorna o conteúdo da tabela em Data Frame.
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


def main():
    data_extracao = datetime.now()
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_PB = PortalTransparencia_PB()
    pt_PB.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_JoaoPessoa()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao)
    logger.info("--- %s segundos ---" % (time.time() - start_time))
