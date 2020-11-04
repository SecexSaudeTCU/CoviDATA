import time
from os import path

import pandas as pd
import zipfile
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


class PT_PR_Scraper(Scraper):
    def scrap(self):
        csv_zip = 'DISPENSAS_INEXIGIBILIDADE_COVID-2020_CSV.zip'
        diretorio = config.diretorio_dados.joinpath('PR').joinpath('portal_transparencia')

        downloader = FileDownloader(diretorio, config.url_pt_PR, csv_zip)
        downloader.download()

        with zipfile.ZipFile(diretorio.joinpath(csv_zip), 'r') as zip_ref:
            zip_ref.extractall(diretorio)

    def consolidar(self, data_extracao):
        return self.__consolidar_aquisicoes(data_extracao), False

    def __consolidar_aquisicoes(self, data_extracao):
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'orgao',
                            consolidacao.DESPESA_DESCRICAO: 'objeto',
                            consolidacao.VALOR_CONTRATO: 'valor_total_solicitacao',
                            consolidacao.CONTRATADO_CNPJ: 'cpf_cnpj_fornecedor',
                            consolidacao.CONTRATADO_DESCRICAO: 'fornecedor'
                            }
        planilha_original = path.join(config.diretorio_dados, 'PR', 'portal_transparencia',
                                      'TB_DISPENSAS_INEXIGIBILIDADE-2020.csv')
        df_original = pd.read_csv(planilha_original, sep=';')

        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                               consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_PR, 'PR', '',
                               data_extracao)
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
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'ÓRGÃO',
                            consolidacao.DESPESA_DESCRICAO: 'OBJETO',
                            consolidacao.CONTRATADO_DESCRICAO: 'CONTRATADO (s)', consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                            consolidacao.DOCUMENTO_NUMERO: 'EMPENHO Nº ',
                            consolidacao.VALOR_CONTRATO: 'VALOR TOTAL/GLOBAL'}
        planilha_original = path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba',
                                      'licitacoes_contratacoes.csv')
        df_original = pd.read_csv(planilha_original, sep=';', header=0, encoding='ISO-8859-1')
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Curitiba_contratacoes
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
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
                            consolidacao.CONTRATANTE_DESCRICAO: 'Órgão',
                            consolidacao.VALOR_CONTRATO: 'Valor R$'}
        planilha_original = path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba',
                                      'aquisicoes.xlsx')
        df_original = pd.read_excel(planilha_original, header=7)
        fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Curitiba_aquisicoes
        codigo_municipio_ibge = get_codigo_municipio_por_nome('Curitiba', 'PR')
        df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
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

        # button = self.driver.find_element_by_class_name('excel')
        button = self.driver.find_element_by_xpath(
            '/html/body/form/div[6]/div[2]/div[1]/div/div/div[4]/div/div[1]/img[1]')
        button.click()

        # Aqui, não é possível confiar na checagem de download da superclasse, uma vez que no mesmo diretório há outros
        # arquivos.
        time.sleep(5)


def pos_processar(df):
    df[consolidacao.MUNICIPIO_DESCRICAO] = 'Curitiba'
    df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'
    return df
