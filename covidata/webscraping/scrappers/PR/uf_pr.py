import datetime
import logging
import os
import time
from os import path

import win32com.client

from covidata import config
from covidata.webscraping.downloader import FileDownloader
from covidata.webscraping.scrappers.PR.consolidacao_PR import consolidar
from covidata.webscraping.selenium.downloader import SeleniumDownloader


class PortalTransparencia_Curitiba(SeleniumDownloader):
    def __init__(self):
        super().__init__(path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba'),
                         config.url_pt_Curitiba_aquisicoes)

    def _executar(self):
        button = self.driver.find_element_by_class_name('excel')
        button.click()


def portal_transparencia_PR():
    agora = datetime.datetime.now()
    mes_atual = agora.month
    mes_anterior = mes_atual - 1

    if mes_atual <= 9:
        mes_atual = '0' + str(mes_atual)

    if mes_anterior <= 9:
        mes_anterior = '0' + str(mes_anterior)

    ano_atual = agora.year

    nome_arquivo_aquisicoes = 'aquisicoes_e_contratacoes_0.xls'
    url_aquisicoes = f'{config.url_pt_PR}{ano_atual}-{mes_anterior}/{nome_arquivo_aquisicoes}'

    pt_PR_aquisicoes = FileDownloader(path.join(config.diretorio_dados, 'PR', 'portal_transparencia'), url_aquisicoes,
                                      nome_arquivo_aquisicoes)
    pt_PR_aquisicoes.download()

    nome_arquivo_dados_abertos = 'dados_abertos.xlsx'
    url_dados_abertos = f'{config.url_pt_PR}{ano_atual}-{mes_atual}/{nome_arquivo_dados_abertos}'

    pt_PR_dados_abertos = FileDownloader(path.join(config.diretorio_dados, 'PR', 'portal_transparencia'),
                                         url_dados_abertos, nome_arquivo_dados_abertos)
    pt_PR_dados_abertos.download()

    return url_aquisicoes, url_dados_abertos


def portal_transparencia_Curitiba():
    pt_contratacoes = FileDownloader(path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba'),
                                     config.url_pt_Curitiba_contratacoes, 'licitacoes_contratacoes.csv')
    pt_contratacoes.download()

    pt_aquisicoes = PortalTransparencia_Curitiba()
    pt_aquisicoes.download()


# TODO: Solução que só funciona no Windows!
def __exportar_arquivo_para_xlsx(diretorio, nome_arquivo_original, nome_novo_arquivo):
    """
    Função que só funciona no Windows, a ser utilizada exepcionalmente para arquivos XLS corruptos.  Esses arquivos não
    não abrem com o uso de bibliotecas Python convencionais. A única solução até o momento é salvar o arquivo no
    formato XLSX.
    :param diretorio: Diretório de origem e de destino.
    :param nome_arquivo_original: Nome do arquivo original.
    :param nome_novo_arquivo: Nome do arquivo a ser gerado.
    :return:
    """
    App = win32com.client.Dispatch("Excel.Application")
    App.Visible = False
    caminho_arquivo_anterior = path.join(diretorio, nome_arquivo_original)
    workbook = App.Workbooks.Open(caminho_arquivo_anterior)
    workbook.ActiveSheet.SaveAs(path.join(diretorio, nome_novo_arquivo), 51)  # 51 is for xlsx
    workbook.Close(SaveChanges=True)
    App.Quit()

    # Remove o arquivo anterior.
    os.remove(caminho_arquivo_anterior)


def main():
    data_extracao = datetime.datetime.now()
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência estadual...')
    start_time = time.time()

    url_aquisicoes, url_dados_abertos = portal_transparencia_PR()

    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Portal de transparência da capital...')
    start_time = time.time()

    portal_transparencia_Curitiba()

    __exportar_arquivo_para_xlsx(path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba'),
                                 'Aquisições_para_enfrentamento_da_pandemia_do_COVID-19_-_Transparência_Curitiba.xls',
                                 'aquisicoes.xlsx')

    logger.info("--- %s segundos ---" % (time.time() - start_time))

    logger.info('Consolidando as informações no layout padronizado...')
    start_time = time.time()
    consolidar(data_extracao, url_aquisicoes, url_dados_abertos)
    logger.info("--- %s segundos ---" % (time.time() - start_time))
