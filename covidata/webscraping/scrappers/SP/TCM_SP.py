from os import path

import locale
import logging
import pandas as pd
import re
import requests
import time
from bs4 import BeautifulSoup
from selenium.common.exceptions import UnexpectedAlertPresentException
from selenium.webdriver.chrome import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout
from covidata.persistencia.dao import persistir
from covidata.webscraping.scrappers.scrapper import Scraper


class TCM_SP_Scraper(Scraper):
    def scrap(self):
        logger = logging.getLogger('covidata')
        logger.info('Tribunal de Contas municipal...')
        start_time = time.time()

        url = config.url_tcm_SP
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'lxml')

        numero_paginas, pagina_atual = self.__get_paginacao(soup)
        colunas, lista_linhas = self.__extrair_tabela(soup)

        driver = self.__configurar_browser()
        driver.get(url)

        for i in range(pagina_atual + 1, numero_paginas + 1):
            linhas = self.__processar_pagina(driver, i, lista_linhas)
            lista_linhas += linhas

        driver.close()
        driver.quit()

        df = pd.DataFrame(lista_linhas, columns=colunas)
        persistir(df, 'tcm', 'licitacoes', 'SP')

        logger.info("--- %s segundos ---" % (time.time() - start_time))

    def __get_paginacao(self, soup):
        paginador = soup.find(id='gridLicitacao_DXPagerBottom')
        texto_paginacao = paginador.contents[1].get_text()
        x = re.findall("Page ([0-9]+) of ([0-9]+)", texto_paginacao)
        pagina_atual = int(x[0][0])
        numero_paginas = int(x[0][1])
        return numero_paginas, pagina_atual

    def __extrair_tabela(self, soup):
        tabela = soup.find(id='gridLicitacao_DXMainTable')
        linhas = tabela.find_all('tr', recursive=False)

        if len(linhas) == 0:
            linhas = tabela.find('tbody').find_all('tr', recursive=False)

        titulos = linhas[0]

        titulos_colunas = titulos.find_all('td')

        indices = range(4, len(titulos_colunas), 3)
        colunas = [titulos_colunas[i].get_text() for i in indices]
        linhas = linhas[1:]
        lista_linhas = []

        for linha in linhas:
            data = linha.find_all("td")
            nova_linha = [data[i].get_text() for i in range(1, len(colunas) + 1)]
            lista_linhas.append(nova_linha)

        return colunas, lista_linhas

    def __configurar_browser(self):
        chromeOptions = webdriver.ChromeOptions()
        # chromeOptions.add_argument('--headless')
        locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")
        driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chromeOptions)
        return driver

    def __processar_pagina(self, driver, i, lista_linhas):
        logger = logging.getLogger("covidata")
        logger.info(f'Processando página {i}...')

        wait = WebDriverWait(driver, 30)
        link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, str(i))))
        driver.execute_script("arguments[0].click();", link)

        # Aguarda o completo carregamento da tela
        time.sleep(20)

        try:
            soup = BeautifulSoup(driver.page_source, 'lxml')
        except UnexpectedAlertPresentException:
            # Tenta novamente
            link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, str(i))))
            driver.execute_script("arguments[0].click();", link)

            # Aguarda o completo carregamento da tela
            time.sleep(20)

            soup = BeautifulSoup(driver.page_source, 'lxml')
        _, linhas = self.__extrair_tabela(soup)
        linhas = self.__checar_linhas_repetidas(driver, linhas, link, lista_linhas)
        return linhas

    def __checar_linhas_repetidas(self, driver, linhas, link, lista_linhas):
        novos_ids = [linha[0] for linha in linhas]
        ids = [lista_linhas[0] for linha in lista_linhas]
        if novos_ids[0] == ids[:-15]:
            # Registros repetidos - recarregamento da página não funcionou, é necessário carregar novamente.
            logger = logging.getLogger('covidata')
            logger.info('Registros repetidos - nova tentativa de carregamento será feita...')
            link.click()

            # Aguarda o completo carregamento da tela
            time.sleep(20)

            soup = BeautifulSoup(driver.page_source, 'lxml')
            _, linhas = self.__extrair_tabela(soup)
        return linhas

    def consolidar(self, data_extracao):
        return self.consolidar_tcm(data_extracao), False

    def consolidar_tcm(self, data_extracao):
        # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
        dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão',
                            consolidacao.DESPESA_DESCRICAO: 'Objeto',
                            consolidacao.VALOR_CONTRATO: 'Valor'}

        # Lê o arquivo "csv" de licitações baixado como um objeto pandas DataFrame
        df_original = pd.read_excel(path.join(config.diretorio_dados, 'SP', 'tcm', 'licitacoes.xls'),
                                    skiprows=list(range(4)),
                                    index_col=0)

        # Chama a função "pre_processar_tcm" definida neste módulo
        df = self.pre_processar_tcm(df_original)

        # Chama a função "consolidar_layout" definida em módulo importado
        df = consolidar_layout(df, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                               consolidacao.TIPO_FONTE_TCM + ' - ' + config.url_tcm_SP, 'SP',
                               get_codigo_municipio_por_nome('São Paulo', 'SP'), data_extracao)

        return df

    def pre_processar_tcm(self, df):
        # Renomeia as colunas especificadas
        df.rename(index=str,
                  columns={'IdLicitacao': 'Identificador Licitação',
                           'Modalidade': 'Modalidade Licitação',
                           'Dt. Publicação': 'Data Publicação',
                           'Licitação': 'Número Licitação',
                           'Processo Externo': 'Número Processo'},
                  inplace=True)

        return df