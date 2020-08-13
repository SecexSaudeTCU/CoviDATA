import logging
import os
import time
from os import path

import pandas as pd

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_DF_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência distrital...')
        start_time = time.time()
        pt_DF = PortalTransparencia_DF()
        pt_DF.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        logger = logging.getLogger('covidata')
        start_time = time.time()
        logger.info('Iniciando consolidação dados Distrito Federal')
        dados = self.__consolidar_dados_portal_transparencia(data_extracao)
        salvar(dados, 'DF')
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def __consolidar_dados_portal_transparencia(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'ÓRGÃO', consolidacao.UG_DESCRICAO: 'ÓRGÃO',
                            consolidacao.CONTRATADO_DESCRICAO: 'CONTRATADO', consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                            consolidacao.DESPESA_DESCRICAO: 'OBJETO',
                            consolidacao.ITEM_EMPENHO_QUANTIDADE: 'QUANTIDADE',
                            consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'VALOR UNITÁRIO',
                            consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'VALOR TOTAL',
                            consolidacao.VALOR_CONTRATO: 'VALOR TOTAL',
                            consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'LOCAL ENTREGA/EXECUÇÃO',
                            consolidacao.NUMERO_PROCESSO: 'PROCESSO', consolidacao.DATA_CELEBRACAO: 'CELEBRAÇÃO'}
        colunas_adicionais = ['INSTRUMENTO CONTRATUAL', 'VIGÊNCIA', 'PUBLICAÇÃO DODF', 'PORTAL COVID-19',
                              'link_convenio',
                              'link_contrato', 'link_processo', 'link_plano_de_trabalho', 'link_justificativa',
                              'lik_edital_credenciamento', 'link_proposta_empresa', 'link_mapa_precos',
                              'link_projeto_basico', 'link_planilha_consolidada', 'link_termo_aditivo',
                              'link_termo_de_referência', 'link_nota_tecnica', 'link_termo_colob_emerg']
        for i in range(1, 11):
            colunas_adicionais.append(f'NE{i}')
            colunas_adicionais.append(f'pdfNE{i}')
            colunas_adicionais.append(f'NE{i}Cancelada')

        planilha_original = path.join(config.diretorio_dados, 'DF', 'portal_transparencia', 'DistritoFederal',
                                      'Dados_Portal_Transparencia_DistritoFederal.xlsx')
        df_original = pd.read_excel(planilha_original)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_DF
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               fonte_dados, 'DF', '', data_extracao)
        df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        return df


# Define a classe referida como herdeira da class "SeleniumDownloader"
class PortalTransparencia_DF(SeleniumDownloader):

    # Sobrescreve o construtor da class "SeleniumDownloader"
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'DF', 'portal_transparencia',
                                   'DistritoFederal'),
                         config.url_pt_DF)

    # Implementa localmente o método interno e vazio da class "SeleniumDownloader"
    def _executar(self):
        # Seleciona o botão "Dados abertos"
        self.driver.find_element_by_xpath('//*[@id="conteudo"]/div[3]/div/div[1]/a').click()

        # On hold por 3 segundos
        time.sleep(5)

        # Lê o arquivo "csv" de nome "PLANILHA-COVID" de contratos baixado como um objeto pandas DataFrame
        df_contratos = pd.read_csv(path.join(config.diretorio_dados, 'DF', 'portal_transparencia',
                                             'DistritoFederal', 'PLANILHA-COVID2.csv'),
                                   sep=';')

        # Cria arquivo "xlsx" e aloca file handler de escrita para a variável "writer"
        with pd.ExcelWriter(path.join(config.diretorio_dados, 'DF', 'portal_transparencia',
                                      'DistritoFederal', 'Dados_Portal_Transparencia_DistritoFederal.xlsx')) as writer:
            # Salva os dados de contratos contidos em "df_contratos" na planilha "Contratos"
            df_contratos.to_excel(writer, sheet_name='Contratos', index=False)

        # Deleta o arquivo "csv" de nome "PLANILHA-COVID"
        os.unlink(path.join(config.diretorio_dados, 'DF', 'portal_transparencia', 'DistritoFederal',
                            'PLANILHA-COVID2.csv'))
