from os import path

import json
import logging
import numpy as np
import os
import pandas as pd
import requests
import time

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper


class PT_RS_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()
        # Realiza o web scraping da tabela principal do portal da transparência do Rio Grande do Sul
        json = self.__baixa_arquivo()

        # Realiza o "parsing" do arquivo JSON para extração dos dados da tabela e retorna um objeto pandas DataFrame
        df_licitacoes = self.__esquadrinha_json(json)

        # Cria diretório do portal da transparência do Rio Grande do Sul
        diretorio_rs = os.path.join(config.diretorio_dados, 'RS', 'portal_transparencia', 'RioGrandeSul')
        if not os.path.exists(diretorio_rs): os.makedirs(diretorio_rs)

        # Salva os dados de licitações contidos em "df_licitacoes" num arquivo "xlsx"
        df_licitacoes.to_excel(os.path.join(diretorio_rs, 'Dados_Portal_Transparencia_RioGrandeSul.xlsx'), index=False)
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def __baixa_arquivo(self):
        """
        Realiza o web scraping do conteúdo da tabela principal do portal e retorna o conteúdo no formato JSON.
        """

        # URL utilizada para obtenção de dados mostrados no painel do PT Rio Grande do Sul
        url = 'https://www.compras.rs.gov.br/transparencia/editais-covid19.json?contexto=Celic&start=0&length=100000&draw=1'

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

        # O arquivo "json" é um dicionário cujos dados constam da chave "data"
        json_content = json['data']

        # Inicializa objetos list para armazenar dados contidos no objeto list "json_content"
        col_edital = []
        col_tipo_objeto = []
        col_modalidade_licitacao = []
        col_central_compras = []
        col_data_abertura = []
        col_situacao_oferta = []
        col_numero_lote = []
        col_situacao_lote = []
        col_numero_item = []
        col_descricao_item = []
        col_quantidade = []
        col_preco_unitario = []
        col_preco_total = []
        col_arrematante = []
        col_CNPJ_arrematante = []
        col_url_documentos = []
        col_url_propostas = []
        col_unidade_valor = []
        col_data_homologacao = []

        # Aloca os valores das referidas chaves do objeto "json_content" aos respectivos objetos list
        for i in range(len(json_content)):
            col_edital.append(json_content[i]['edital'])
            col_tipo_objeto.append(json_content[i]['tipoObjeto'])
            col_modalidade_licitacao.append(json_content[i]['tipoObjeto'])
            col_central_compras.append(json_content[i]['centralDeCompras'])
            col_data_abertura.append(json_content[i]['dataDeAbertura'])
            col_situacao_oferta.append(json_content[i]['situacaoOferta'])
            col_numero_lote.append(json_content[i]['numeroLote'])
            col_situacao_lote.append(json_content[i]['situacaoLote'])
            col_numero_item.append(json_content[i]['numeroItem'])
            col_descricao_item.append(json_content[i]['itemDescricao'])
            col_quantidade.append(json_content[i]['quantidade'])
            col_preco_unitario.append(json_content[i]['valorUnitario'])
            col_preco_total.append(json_content[i]['valorTotal'])
            col_arrematante.append(json_content[i]['arrematante'])
            col_CNPJ_arrematante.append(json_content[i]['arrematanteCNPJ'])
            col_url_documentos.append(json_content[i]['urlDocumentos'])
            col_url_propostas.append(json_content[i]['urlPropostas'])
            col_unidade_valor.append(json_content[i]['unidadeDeValor'])
            col_data_homologacao.append(json_content[i]['dataHomologacao'])

        # Armazena os dados dos objetos list como um objeto pandas DataFrame
        df = pd.DataFrame(list(zip(col_edital, col_tipo_objeto, col_modalidade_licitacao,
                                   col_central_compras, col_data_abertura, col_situacao_oferta,
                                   col_numero_lote, col_situacao_lote, col_numero_item,
                                   col_descricao_item, col_quantidade, col_preco_unitario,
                                   col_preco_total, col_arrematante, col_CNPJ_arrematante,
                                   col_url_documentos, col_url_propostas, col_unidade_valor,
                                   col_data_homologacao)),
                          columns=['Número Licitação', 'Tipo Objeto', 'Modalidade Licitação',
                                   'Central Compras', 'Data Abertura', 'Situação Oferta',
                                   'Número Lote', 'Situação Lote', 'Número Item',
                                   'Descrição Item', 'Quantidade', 'Preço Unitário',
                                   'Preço Total', 'Nome Arrematante', 'CNPJ Arrematante',
                                   'URL Documentos', 'URL Propostas', 'Unidade Valor',
                                   'Data Homologação'])

        return df

    def consolidar(self, data_extracao):
        return self.consolidar_pt_RS(data_extracao), False

    def consolidar_pt_RS(self, data_extracao):

        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Central Compras',
                            consolidacao.DESPESA_DESCRICAO: 'Descrição Item',
                            consolidacao.VALOR_CONTRATO: 'Preço Total',
                            consolidacao.CONTRATADO_DESCRICAO: 'Nome Arrematante',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ Arrematante'}

        # Lê o arquivo "xlsx" de licitações baixado como um objeto pandas DataFrame
        df_original = pd.read_excel(path.join(config.diretorio_dados, 'RS', 'portal_transparencia',
                                              'RioGrandeSul', 'Dados_Portal_Transparencia_RioGrandeSul.xlsx'))

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_RS, 'RS', '',
                               data_extracao, self.pos_processar)

        return df

    def pos_processar(self, df):
        # Remove a notação científica
        df[consolidacao.CONTRATADO_CNPJ] = df[consolidacao.CONTRATADO_CNPJ].astype(np.int64)
        df[consolidacao.CONTRATADO_CNPJ] = df[consolidacao.CONTRATADO_CNPJ].astype(str)

        return df
