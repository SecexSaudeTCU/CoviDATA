import json
import logging
import os
import time
import pandas as pd
import requests

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from os import path

class PT_BoaVista_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')

        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        # Realiza o web scraping da tabela principal do portal da transparência de Boa VIsta
        json = self.__baixa_arquivo()

        # Realiza o parsing do arquivo JSON para extração dos dados da tabela e retorna um objeto pandas DataFrame
        df_contratos = self.__esquadrinha_json(json)

        # Cria diretório do portal da transparência de Boa Vista
        diretorio_bv = os.path.join(config.diretorio_dados, 'RR', 'portal_transparencia', 'BoaVista')
        if not os.path.exists(diretorio_bv): os.makedirs(diretorio_bv)

        # Salva os dados de contratos contidos em "df_contratos" num arquivo "xlsx"
        df_contratos.to_excel(os.path.join(diretorio_bv, 'Dados_Portal_Transparencia_BoaVista.xlsx'), index=False)
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        logger = logging.getLogger('covidata')
        logger.info('Iniciando consolidação dados Boa Vista')

        consolidacao_pt_BoaVista = self.consolidar_pt_BoaVista(data_extracao)

        return consolidacao_pt_BoaVista, False

    def __baixa_arquivo(self):
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

    def __esquadrinha_json(self, json):
        """
        Realiza o parsing do arquivo JSON e retorna o conteúdo da tabela em Data Frame.
        """

        # O arquivo "json" é um dicionário cuja única chave é "processos"
        json_content = json['processos']

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

            if len(json_content[i]['items']) > 0:
                col_descricao_produto.append(json_content[i]['items'][0]['description'])
                col_quantidade_produto.append(json_content[i]['items'][0]['qtd'])
                col_preco_unitario_produto.append(json_content[i]['items'][0]['value'])

            if len(json_content[i]['vencedores']) > 0:
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

    def consolidar_pt_BoaVista(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.DESPESA_DESCRICAO: 'Objeto Licitação',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                            consolidacao.CONTRATADO_DESCRICAO: 'Contratado',
                            consolidacao.VALOR_CONTRATO: 'Valor Contrato',
                            consolidacao.DATA_CELEBRACAO: 'Data Contrato'}

        # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
        colunas_adicionais = ['Número Licitação', 'Situação Licitação', 'Modalidade Licitacao',
                              'Data Abertura', 'Data Publicação', 'Descrição Produto',
                              'Quantidade Produto', 'PU Produto', 'Prazo Execução']

        # Lê o arquivo "xlsx" de despesas baixado como um objeto pandas DataFrame
        df_original = pd.read_excel(path.join(config.diretorio_dados, 'RR', 'portal_transparencia',
                                              'BoaVista', 'Dados_Portal_Transparencia_BoaVista.xlsx'))

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_BoaVista, 'RR',
                               get_codigo_municipio_por_nome('Boa Vista', 'RR'), data_extracao,
                               self.pos_processar_pt_BoaVista)

        return df

    def pos_processar_pt_BoaVista(self, df):
        for i in range(len(df)):
            cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

            if len(str(cpf_cnpj)) >= 14:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
            else:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF

        df[consolidacao.MUNICIPIO_DESCRICAO] = 'Boa Vista'
        return df


