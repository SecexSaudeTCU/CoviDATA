import logging
import time
from os import path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.persistencia.dao import persistir
from covidata.webscraping.scrappers.scrapper import Scraper
from requests.packages.urllib3.util.retry import Retry


class PT_RN_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/50.0.2661.102 Safari/537.36'}
        # page = requests.get(config.url_pt_RN, headers=headers, verify=False)
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        page = session.get(config.url_pt_RN, headers=headers, verify=False)

        soup = BeautifulSoup(page.content, 'html.parser')
        tabela = soup.find_all('table')[1]
        titulos_colunas = tabela.find_all('thead')[0].find_all('th')
        colunas = [titulo_coluna.get_text() for titulo_coluna in titulos_colunas]

        linhas = tabela.find_all('tbody')[0].find_all('tr')
        linhas_df = []

        for linha in linhas:
            tds = linha.find_all('td')
            valores = [td.get_text().strip() for td in tds]
            linhas_df.append(valores)

        df = pd.DataFrame(linhas_df, columns=colunas)
        persistir(df, 'portal_transparencia', 'compras_e_servicos', 'RN')

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_pt_RN(data_extracao), False

    def consolidar_pt_RN(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Contratante',
                            consolidacao.UG_DESCRICAO: 'Contratante',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.CONTRATADO_DESCRICAO: 'Contratado (a)',
                            consolidacao.NUMERO_CONTRATO: 'N. Contrato',
                            consolidacao.VALOR_CONTRATO: 'Valor do Contrato (R$)',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ/CPF',
                            consolidacao.NUMERO_PROCESSO: 'N. Processo',
                            consolidacao.FUNDAMENTO_LEGAL: 'Fundamento Legal',
                            consolidacao.FONTE_RECURSOS_DESCRICAO: 'Fonte do Recurso',
                            consolidacao.VALOR_PAGO: 'Valor Pago (R$)', consolidacao.DATA_ASSINATURA: 'Data Assinatura',
                            consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'Local de execução'}

        df_original = pd.read_excel(
            path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'compras_e_servicos.xls'), header=4)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_RN, 'RN', '',
                               data_extracao, self.pos_processar_pt)
        return df

    def pos_processar_pt(self, df):
        for i in range(len(df)):
            cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

            if len(cpf_cnpj) == 14:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
            elif len(cpf_cnpj) > 14:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

        return df
