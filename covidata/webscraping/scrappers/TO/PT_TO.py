import datetime
import logging
import time

import requests
from bs4 import BeautifulSoup, Tag

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import salvar, consolidar_layout
from covidata.persistencia.dao import persistir
from covidata.webscraping.scrappers.scrapper import Scraper
import pandas as pd
from os import path

class PT_TO_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()
        self.pt_TO()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def pt_TO(self):
        linhas = []
        page = requests.get(config.url_pt_TO)
        soup = BeautifulSoup(page.content, 'html.parser')
        linha_titulos = soup.find(id='tit_consulta_contrato_covid_2__SCCS__1')
        colunas = linha_titulos.find_all('td')
        nomes_colunas = ['Órgão']

        for coluna in colunas:
            nome_coluna = coluna.get_text().strip()

            if nome_coluna != '':
                nomes_colunas.append(nome_coluna)

        for sibling in linha_titulos.next_siblings:
            if isinstance(sibling, Tag):
                # se for o nome do órgão
                texto = sibling.get_text().strip()
                if texto.startswith('Órgão - '):
                    nome_orgao = texto[len('Órgão - '): len(texto)]
                elif (not ('Total do Processo/Contrato' in texto)) and (
                not ('Total do Tipo de Contratação' in texto)) and (
                        not ('Total do Órgão' in texto)):
                    trs = sibling.find_all('tr')
                    for tr in trs:
                        tds = tr.find_all('td')
                        if len(tds) >= len(nomes_colunas):
                            # indicativo de linha de conteúdo
                            linha = [nome_orgao]

                            for td in tds:
                                texto_tag = td.get_text().strip()
                                if texto_tag != '':
                                    linha.append(texto_tag)

                            # A quinta coluna, caso preenchida, tem que estar no formato data, e a sexta coluna,
                            # caso prenchida, tem que ser um número.  Estas verificações têm por objetivo contornar o
                            # problema das colunas não obrigatórias.
                            try:
                                datetime.datetime.strptime(linha[5], '%d/%m/%Y')
                            except ValueError:
                                linha.insert(5, '')

                            if not linha[6].isnumeric():
                                linha.insert(6, '')

                            linhas.append(linha)

        df = pd.DataFrame(linhas, columns=nomes_colunas)
        persistir(df, 'portal_transparencia', 'contratos', 'TO')

    def consolidar(self, data_extracao):
        logger = logging.getLogger('covidata')
        logger.info('Iniciando consolidação dados Tocantins')

        contratos = self.__consolidar_contratos(data_extracao)

        salvar(contratos, 'TO')
        return contratos

    def __consolidar_contratos(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão', consolidacao.CONTRATADO_CNPJ: 'CPF/CNPJ',
                            consolidacao.CONTRATADO_DESCRICAO: 'Contratado', consolidacao.UG_DESCRICAO: 'Órgão',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.ITEM_EMPENHO_UNIDADE_MEDIDA: 'Unidade',
                            consolidacao.ITEM_EMPENHO_QUANTIDADE: 'Qtde',
                            consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'Valor',
                            consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'Total', consolidacao.TIPO_DOCUMENTO: 'Instrumento',
                            consolidacao.DOCUMENTO_NUMERO: 'Número', consolidacao.DOCUMENTO_DATA: 'Data',
                            consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'Local de Entrega',
                            consolidacao.PRAZO_EM_DIAS: 'Prazo (dias)', consolidacao.NUMERO_PROCESSO: 'Processo'}
        colunas_adicionais = ['Modalidade']
        planilha_original = path.join(config.diretorio_dados, 'TO', 'portal_transparencia', 'contratos.xls')
        df_original = pd.read_excel(planilha_original, header=4)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_TO
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               fonte_dados, 'TO', '', data_extracao, self.pos_processar_contratos)
        return df

    def pos_processar_contratos(self, df):
        for i in range(0, len(df)):
            cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

            if len(cpf_cnpj) == 14:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
            elif len(cpf_cnpj) > 14:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

        return df