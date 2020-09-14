import time
from os import path

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.util.excel import exportar_arquivo_para_xlsx
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.scrapper import Scraper
from covidata.webscraping.selenium.downloader import SeleniumDownloader


def pos_processar(df):
    df[consolidacao.MUNICIPIO_DESCRICAO] = 'Curitiba'
    df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'

    # Como no caso de PR, em nenhum momento a informação de CNPJ é fornecida, sinalizar tipo para PJ para que, na
    # consolidação geral seja executada a busca por CNPJs.
    df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    return df


class PT_CuritibaContratacoes_Scraper(Scraper):
    def scrap(self):
        pt_contratacoes = FileDownloader(path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba'),
                                         config.url_pt_Curitiba_contratacoes, 'licitacoes_contratacoes.csv')
        pt_contratacoes.download()

    def consolidar(self, data_extracao):
        licitacoes_capital = self.__consolidar_licitacoes_capital(data_extracao)
        return licitacoes_capital, False

    def __consolidar_licitacoes_capital(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'ÓRGÃO', consolidacao.UG_DESCRICAO: 'ÓRGÃO',
                            consolidacao.DESPESA_DESCRICAO: 'OBJETO',
                            consolidacao.MOD_APLIC_DESCRICAO: 'MODALIDADE DA CONTRATAÇÃO',
                            consolidacao.MOD_APLICACAO_COD: 'Nº DA MODALIDADE',
                            consolidacao.CONTRATADO_DESCRICAO: 'CONTRATADO (s)', consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                            consolidacao.ITEM_EMPENHO_QUANTIDADE: 'QUANTIDADE',
                            consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: ' VALOR UNITÁRIO ',
                            consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'VALOR TOTAL/GLOBAL',
                            consolidacao.VALOR_EMPENHADO: 'EMPENHADO', consolidacao.DOCUMENTO_NUMERO: 'EMPENHO Nº ',
                            consolidacao.VALOR_LIQUIDADO: 'VALOR LIQUIDADO', consolidacao.VALOR_PAGO: 'VALOR PAGO',
                            consolidacao.FONTE_RECURSOS_DESCRICAO: 'FONTE DE RECURSOS',
                            consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'LOCAL DA EXECUÇÃO/ENTREGA',
                            consolidacao.NUMERO_CONTRATO: 'Nº CONTRATO',
                            consolidacao.DATA_PUBLICACAO: 'PUBLICAÇÃO NO DIÁRIO OFICIAL'}
        colunas_adicionais = ['PROTOCOLO', 'VIGENCIA DO CONTRATO', 'VALOR CANCELADO', 'BOLETIM DE PAGAMENTO', 'A PAGAR']
        planilha_original = path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba',
                                      'licitacoes_contratacoes.csv')
        df_original = pd.read_csv(planilha_original, sep=';', header=0, encoding='ISO-8859-1')
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Curitiba_contratacoes
        df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'PR', get_codigo_municipio_por_nome('Curitiba', 'PR'), data_extracao,
                               pos_processar)
        return df


class PT_CuritibaAquisicoes_Scraper(Scraper):
    def scrap(self):
        pt_aquisicoes = PortalTransparencia_Curitiba()
        pt_aquisicoes.download()

        exportar_arquivo_para_xlsx(path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba'),
                                   'Aquisições_para_enfrentamento_da_pandemia_do_COVID-19_-_Transparência_Curitiba.xls',
                                   'aquisicoes.xlsx')

    def consolidar(self, data_extracao):
        aquisicoes_capital = self.__consolidar_aquisicoes_capital(data_extracao)
        return aquisicoes_capital, False

    def __consolidar_aquisicoes_capital(self, data_extracao):
        dicionario_dados = {consolidacao.DOCUMENTO_DATA: 'Data', consolidacao.DOCUMENTO_NUMERO: 'Documento/Empenho',
                            consolidacao.FUNCAO_DESCRICAO: 'Função', consolidacao.SUBFUNCAO_DESCRICAO: 'Subfunção',
                            consolidacao.FONTE_RECURSOS_DESCRICAO: 'Fonte',
                            consolidacao.MOD_APLIC_DESCRICAO: 'Modalidade',
                            consolidacao.ELEMENTO_DESPESA_DESCRICAO: 'Elemento',
                            consolidacao.CONTRATANTE_DESCRICAO: 'Órgão', consolidacao.UG_DESCRICAO: 'Órgão',
                            consolidacao.VALOR_CONTRATO: 'Valor R$',
                            consolidacao.CATEGORIA_ECONOMICA_DESCRICAO: 'Categoria',
                            consolidacao.GND_DESCRICAO: 'Grupo'}
        planilha_original = path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba',
                                      'aquisicoes.xlsx')
        df_original = pd.read_excel(planilha_original, header=7)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Curitiba_aquisicoes
        codigo_municipio_ibge = get_codigo_municipio_por_nome('Curitiba', 'PR')
        df = consolidar_layout([], df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               fonte_dados, 'PR', codigo_municipio_ibge, data_extracao, pos_processar)
        return df


class PortalTransparencia_Curitiba(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba'),
                         config.url_pt_Curitiba_aquisicoes)

    def _executar(self):
        # Seleciona o campo "Data Início" e seta a data de início de busca
        campo_data_inicial = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.NAME, "ctl00$cphMasterPrincipal$txtDataInicial")))
        campo_data_inicial.send_keys(Keys.HOME)
        campo_data_inicial.send_keys('01032020')

        #button = self.driver.find_element_by_class_name('excel')
        button = self.driver.find_element_by_xpath('/html/body/form/div[6]/div[2]/div[1]/div/div/div[4]/div/div[1]/img[1]')
        button.click()

        # Aqui, não é possível confiar na checagem de download da superclasse, uma vez que no mesmo diretório há outros
        # arquivos.
        time.sleep(5)
