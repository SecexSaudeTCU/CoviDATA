import json
import logging
import time
from datetime import datetime
from glob import glob
from os import path

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader
import pandas as pd


class PT_GO_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')

        logger.info('Portal de transparência estadual...')
        start_time = time.time()
        pt_GO = FileDownloader(path.join(config.diretorio_dados, 'GO', 'portal_transparencia'), config.url_pt_GO,
                               'aquisicoes.csv')
        pt_GO.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_aquisicoes(data_extracao), False

    def __consolidar_aquisicoes(self, data_extracao):
        dicionario_dados = {consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.VALOR_CONTRATO: 'Valor',
                            consolidacao.CONTRATADO_DESCRICAO: 'Credor',
                            consolidacao.CONTRATADO_CNPJ: 'CPF_CNPJ_COTACOES'}
        planilha_original = path.join(config.diretorio_dados, 'GO', 'portal_transparencia', 'aquisicoes.csv')
        df_original = pd.read_csv(planilha_original)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_GO
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               fonte_dados, 'GO', '', data_extracao)
        return df


class PT_Goiania_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Portal de transparência da capital...')
        start_time = time.time()
        pt_despesas_Goiania = PT_Despesas_Goiania()
        pt_despesas_Goiania.download()
        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def consolidar(self, data_extracao):
        return self.__consolidar_pt_despesas_Goiania(data_extracao), False

    def __consolidar_pt_despesas_Goiania(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.DOCUMENTO_NUMERO: 'Empenho',
                            consolidacao.DOCUMENTO_DATA: 'Data Empenho',
                            consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                            consolidacao.CONTRATADO_DESCRICAO: 'Nome Favorecido',
                            consolidacao.CONTRATANTE_DESCRICAO: 'Órgão'}

        # Obtém objeto list dos arquivos armazenados no path passado como argumento para a função nativa "glob"
        list_files = glob(path.join(config.diretorio_dados, 'GO', 'portal_transparencia', 'Goiania/*'))

        # Obtém o primeiro elemento do objeto list que corresponde ao nome do arquivo "json" baixado
        file_name = list_files[0]

        # Armazena o conteúdo do arquivo "json" de nome "file_name" de despesas em um file handler
        f = open(file_name)

        # Carrega os dados no formato json
        data_original = json.load(f)
        # data_original = json.load(f, encoding='IS0-8859-1')

        # Chama a função "pre_processar_pt_despesas_Goiania" definida neste módulo
        df = self.pre_processar_pt_despesas_Goiania(data_original)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Goiania_despesas,
                               'GO',
                               get_codigo_municipio_por_nome('Goiânia', 'GO'), data_extracao)

        return df

    def pre_processar_pt_despesas_Goiania(self, json):
        # Inicializa objetos list para armazenar dados contidos no "dicionário" "json"
        col_empenho = []
        col_data_empenho = []
        col_natureza_despesa = []
        col_cnpj = []
        col_nome_favorecido = []
        col_nome_orgao = []
        col_valor_empenhado = []
        col_valor_liquidado = []
        col_valor_pago = []

        # Aloca os valores das referidas chaves do "dicionário" "json" aos respectivos objetos list
        for i in range(len(json)):
            col_empenho.append(json[i]['Empenho'])
            col_data_empenho.append(json[i]['DataEmpenho'])
            col_natureza_despesa.append(json[i]['NaturezaDespesa'])
            col_cnpj.append(json[i]['CNPJ'])
            col_nome_favorecido.append(json[i]['NmFavorecido'])
            col_nome_orgao.append(json[i]['NmOrgao'])
            col_valor_empenhado.append(json[i]['VlEmpenhado'])
            col_valor_liquidado.append(json[i]['VlLiquidado'])
            col_valor_pago.append(json[i]['VlPago'])

        # Armazena os dados dos objetos list como um objeto pandas DataFrame
        df = pd.DataFrame(list(zip(col_empenho, col_data_empenho, col_natureza_despesa, col_cnpj,
                                   col_nome_favorecido, col_nome_orgao, col_valor_empenhado,
                                   col_valor_liquidado, col_valor_pago)),
                          columns=['Empenho', 'Data Empenho', 'Natureza Despesa', 'CNPJ',
                                   'Nome Favorecido', 'Nome Órgão', 'Valor Empenhado',
                                   'Valor Liquidado', 'Valor Pago'])

        # Slice da coluna de string "Data Empenho" apenas os caracteres de data (10 primeiros), em seguida...
        # os converte para datatime, em seguida para date apenas e, por fim, para string no formato "dd/mm/aaaa"
        df['Data Empenho'] = \
            df['Data Empenho'].apply(lambda x: datetime.strptime(x[:10], '%Y-%m-%d').date().strftime('%d/%m/%Y'))

        return df


class PT_Despesas_Goiania(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'GO', 'portal_transparencia', 'Goiania'),
                         config.url_pt_Goiania_despesas)

    def _executar(self):
        # Cria um objeto da class "WebDriverWait"
        wait = WebDriverWait(self.driver, 45)

        # Entra no iframe de id "modulo_portal"
        iframe = wait.until(EC.visibility_of_element_located((By.ID, 'modulo_portal')))
        self.driver.switch_to.frame(iframe)

        # Seleciona o menu dropdown "Exportar" e seta a opção "JSON"
        select = Select(self.driver.find_element_by_xpath(
            '//*[@id="WebPatterns_wt2_block_wtMainContent_DespesasWebBlocks_wt3_block_wtselExportar"]'))
        select.select_by_visible_text('JSON')
