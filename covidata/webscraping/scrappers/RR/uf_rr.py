import os
from os import path
import logging
import time
from datetime import datetime
import requests
import json

import pandas as pd

from covidata import config
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.RR.consolidacao_RR import consolidar



def pt_BoaVista():

    # Realiza o web scraping da tabela principal do portal da transparência de Boa VIsta
    json = __baixa_arquivo()

    # Realiza o "parsing" do arquivo JSON para extração dos dados da tabela e retorna um objeto pandas DataFrame
    df_contratos = __esquadrinha_json(json)

    # Cria diretório do portal da transparência de Boa Vista
    diretorio_bv = os.path.join(config.diretorio_dados, 'RR', 'portal_transparencia', 'BoaVista')
    if not os.path.exists(diretorio_bv): os.makedirs(diretorio_bv)

    # Salva os dados de despesas contidos em "df_contratos" num arquivo "xlsx"
    df_contratos.to_excel(os.path.join(diretorio_bv, 'Dados_Portal_Transparencia_BoaVista.xlsx'), index=False)


def __baixa_arquivo():
    """
    Realiza o web scraping do conteúdo da tabela principal do portal e retorna o conteúdo no formato JSON.
    """

    # URL utilizada para obtenção de dados mostrados no painel do PT Boa Vista
    url = 'https://transparencia.boavista.rr.gov.br/covid-19/json'

    # Cabeçalhos da requisição GET
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}

    # Realizar a requisição GET para obtenção dos dados da tabela
    r = requests.get(url, headers=headers, verify=False)

    # Obtém os dados em formato JSON com o conteúdo da tabela
    d = json.loads(r.content)

    return d


def __esquadrinha_json(json):
    """
    Realiza o parsing do arquivo JSON e retorna o conteúdo da tabela em Data Frame.
    """

    # O arquivo "json" é um dicionário cuja única chave é "processos"
    json_content = json['processos']
    print(len(json_content))

    # Inicializa objetos list para armazenar dados contidos no objeto list "json_content"
    col_numero_licitacao = []
    col_situacao_licitacao = []
    col_modalidade_licitacao = []
    col_data_abertura = []
    col_data_publicacao = []
    col_objeto_licitacao = []
    col_descricao_produto = []
    col_quantidade_produto = []
    col_preco_unitario_produto = []
    col_cnpj = []
    col_nome_contratado = []
    col_data_contrato = []
    col_prazo_execucao = []
    col_valor_contrato = []

    # Aloca os valores das referidas chaves do objeto "json_content" aos respectivos objetos list
    for i in range(len(json_content)):
        col_numero_licitacao.append(json_content[i]['numero_licitacao'])
        col_situacao_licitacao.append(json_content[i]['situacao'])
        col_modalidade_licitacao.append(json_content[i]['modalidade'])
        col_data_abertura.append(json_content[i]['data_abertura'])
        col_data_publicacao.append(json_content[i]['data_publicacao'])
        col_objeto_licitacao.append(json_content[i]['objeto'])
        col_descricao_produto.append(json_content[i]['items'][0]['description'])
        col_quantidade_produto.append(json_content[i]['items'][0]['qtd'])
        col_preco_unitario_produto.append(json_content[i]['items'][0]['value'])
        col_cnpj.append(json_content[i]['vencedores'][0]['cnpj'])
        col_nome_contratado.append(json_content[i]['vencedores'][0]['fantasia'])
        col_data_contrato.append(json_content[i]['vencedores'][0]['data_celebracao'])
        col_prazo_execucao.append(json_content[i]['vencedores'][0]['prazo'])
        col_valor_contrato.append(json_content[i]['vencedores'][0]['valor_contrato'])

    # Armazena os dados dos objetos list como um objeto pandas DataFrame
    df = pd.DataFrame(list(zip(col_numero_licitacao, col_situacao_licitacao, col_modalidade_licitacao,
                               col_data_abertura, col_data_publicacao, col_objeto_licitacao,
                               col_descricao_produto, col_quantidade_produto, col_preco_unitario_produto,
                               col_cnpj, col_nome_contratado, col_data_contrato, col_prazo_execucao,
                               col_valor_contrato)),
                      columns=['Número Licitação', 'Situação Licitação', 'Modalidade Licitacao',
                               'Data Abertura', 'Data Publicação', 'Objeto Licitação',
                               'Descrição Produto', 'Quantidade Produto', 'PU Produto',
                               'CNPJ', 'Contratado', 'Data Contrato', 'Prazo Execução',
                               'Valor Contrato'])

    return df


def main():
    data_extracao = datetime.now()
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()
    pt_RR = FileDownloader(path.join(config.diretorio_dados, 'RR', 'portal_transparencia', 'Roraima'), config.url_pt_RR,
                           'Dados_Portal_Transparencia_Roraima.xls')
    pt_RR.download()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()
    pt_BoaVista()
    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao)
    logger.info("--- %s segundos ---" % (time.time() - start_time))
