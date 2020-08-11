import logging
import time
from glob import glob
from os import path

import pandas as pd

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PT_RN_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência estadual...')
        start_time = time.time()
        pt_RN = PortalTransparencia_RN()
        pt_RN.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.consolidar_pt_RN(data_extracao)

    def consolidar_pt_RN(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Contratante',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.CONTRATADO_DESCRICAO: 'Contratado (a)',
                            consolidacao.DOCUMENTO_NUMERO: 'N. Contrato',
                            consolidacao.VALOR_CONTRATO: 'Valor do Contrato (R$)',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ/CPF',
                            consolidacao.FONTE_RECURSOS_DESCRICAO: 'Fonte do Recurso',
                            consolidacao.VALOR_PAGO: 'Valor Pago (R$)', consolidacao.DATA_ASSINATURA: 'Data Assinatura'}

        # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
        colunas_adicionais = ['N. Processo', 'Modalidade', 'Fundamento Legal', 'Vigência', 'Valor anulado (R$)']

        # Obtém objeto list dos arquivos armazenados no path passado como argumento para a função nativa "glob"
        list_files = glob(path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'RioGrandeNorte/*'))

        # Obtém o primeiro elemento do objeto list que corresponde ao nome do arquivo "csv" baixado
        file_name = list_files[0]

        # Lê o arquivo "csv" de nome "file_name" de contratos baixado como um objeto pandas DataFrame
        # Usa o parâmetro "error_bad_lines" como "False" para ignorar linhas com problema do arquivo "csv" (primeira solução)
        df_original = pd.read_csv(
            path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'RioGrandeNorte', file_name),
            sep=';', error_bad_lines=False)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_RN, 'RN', '',
                               data_extracao, self.pos_processar_pt)
        return df

    def pos_processar_pt(self, df):
        for i in range(len(df)):
            cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

            if len(cpf_cnpj) >= 14:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
            else:
                df.loc[i, consolidacao.FAVORECIDO_TIPO] = 'CPF/RG'

        return df


# Define a classe referida como herdeira da class "SeleniumDownloader"
class PortalTransparencia_RN(SeleniumDownloader):

    # Sobrescreve o construtor da class "SeleniumDownloader"
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'RN', 'portal_transparencia', 'RioGrandeNorte'),
                         config.url_pt_RN,
                         #browser_option='--start-maximized'
                         )

    # Implementa localmente o método interno e vazio da class "SeleniumDownloader"
    def _executar(self):

        # Seleciona o botão em forma de planilha do Excel
        self.driver.find_element_by_xpath('//*[@id="DataTables_Table_0_wrapper"]/div[1]/button[2]/span/i').click()