import re
import time

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import UnexpectedAlertPresentException
from webdriver_manager.chrome import ChromeDriverManager

from covidata import config
import pandas as pd
from covidata.persistencia.dao import persistir2
import locale
import logging

import re
from urllib.parse import urlparse
from urllib.parse import parse_qs


def main():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência da capital...')
    start_time = time.time()

    url = config.url_pt_PortoAlegre
    parsed_uri = urlparse(url)
    url_base = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)

    page = requests.get(url)
    parser = BeautifulSoup(page.content, 'lxml')

    proximas_paginas = __get_paginacao(parser)

    nomes_colunas_documentos = []
    linhas_df = []

    registros = parser.findAll('div', {'class': 'item-registro xs-col-12'})

    for registro in registros:
        textos = re.split('\r\n', registro.findAll('h2')[0].get_text().strip())
        codigo_licitacao = textos[0]

        conteudo = registro.findAll('div')[0]
        divs = conteudo.findAll('div')
        div_descricao_registro = divs[0]
        objeto = div_descricao_registro.findAll('p')[0].get_text()
        div_detalhes = divs[1]
        spans = div_detalhes.findAll('span')
        tipo = spans[0].get_text()
        status = spans[1].get_text()

        # página de detalhes
        parser_detalhes = processar_detalhes(codigo_licitacao, linhas_df, objeto, spans, status, tipo, url_base)

        # documentos
        linhas_df_documentos, nomes_colunas_documentos = processar_documentos(codigo_licitacao,
                                                                              nomes_colunas_documentos, parser_detalhes,
                                                                              url_base)

        # itens
        linhas_df_itens = processar_itens(codigo_licitacao, parser_detalhes)

    nomes_colunas = ['Número da licitação', 'Objeto', 'Tipo', 'Status', 'Aplicar o Decreto 10.024/2019',
                     'Início de propostas', 'Final de propostas', 'Limite para impugnações', 'Data de Abertura',
                     'Pregoeiro', 'Autoridade Competente', 'Apoio', 'Origem dos Recursos', 'Operação']
    df = pd.DataFrame(linhas_df, columns=nomes_colunas)
    print(df.head())

    df_documentos = pd.DataFrame(linhas_df_documentos, columns=nomes_colunas_documentos)
    print(df_documentos)

    nomes_colunas_itens = ['Número da Licitação', 'Item', 'Descrição', 'Unidade', 'Quantidade', 'Melhor Lance (R$',
                           'Situação', 'Modalidade']
    df_itens = pd.DataFrame(linhas_df_itens, columns=nomes_colunas_itens)
    print(df_itens)

    print("--- %s segundos ---" % (time.time() - start_time))


def processar_detalhes(codigo_licitacao, linhas_df, objeto, spans, status, tipo, url_base):
    dados_adicionais = spans[2]
    url_detalhes = dados_adicionais.findAll('a', {'title': 'Dados do Processo'})[0].attrs['href']
    url_detalhes = url_base + url_detalhes

    driver_detalhes = __get_browser(url_detalhes)
    html = driver_detalhes.page_source
    driver_detalhes.close()
    driver_detalhes.quit()

    parser_detalhes = BeautifulSoup(html, 'lxml')
    dados_licitacao = parser_detalhes.findAll('p', {'class': 'ff1 f400 fs16 fcW'})[0]
    bs = dados_licitacao.findAll('b')
    aplicar_decreto = bs[1].next_sibling.string.strip()
    inicio_propostas = bs[3].next_sibling.string.strip()
    final_propostas = bs[4].next_sibling.string.strip()
    limite_impugnacoes = bs[5].next_sibling.string.strip()
    data_abertura = bs[6].next_sibling.string.strip()
    pregoeiro = bs[7].next_sibling.string.strip()
    autoridade_competente = bs[8].next_sibling.string.strip()
    apoio = bs[9].next_sibling.string.strip()
    origem_recursos = bs[11].next_sibling.string.strip()
    operacao = bs[12].next_sibling.string.strip()

    linhas_df.append([codigo_licitacao, objeto, tipo, status, aplicar_decreto, inicio_propostas, final_propostas,
                      limite_impugnacoes, data_abertura, pregoeiro, autoridade_competente, apoio, origem_recursos,
                      operacao])

    return parser_detalhes


def processar_documentos(codigo_licitacao, nomes_colunas_documentos, parser_detalhes, url_base):
    div_documentos = parser_detalhes.findAll('div', {'class': 'col-xs-12 col-sm-6 bgMidGreen'})[0]
    url_documentos = div_documentos.findAll('a', {'title': 'Atas e demais documentos do processo'})[0].attrs['href']
    url_documentos = url_base + url_documentos
    parsed_url_documentos = urlparse(url_documentos)
    chave = parse_qs(parsed_url_documentos.query)['ttCD_CHAVE'][0]

    driver_documentos = __get_browser(url_documentos)
    html = driver_documentos.page_source
    driver_documentos.close()
    driver_documentos.quit()

    parser_documentos = BeautifulSoup(html, 'lxml')
    span = parser_documentos.findAll('span', {'class': 'green'})
    numero_processo_interno = span[0].findAll('b')[0].get_text()
    tabela = parser_documentos.findAll('table')[0]
    cabecalho = tabela.findAll('th')
    nomes_colunas_documentos = ['Número da Licitação', 'Número do Processo Interno'] + \
                               [coluna.get_text() for coluna in cabecalho] + \
                               ['Comentário']
    linhas_documentos = tabela.findAll('tbody')[0].findAll('tr')
    linhas_df_documentos = []
    for linha_documento in linhas_documentos:
        tds = linha_documento.findAll('td')
        a = tds[0].findAll('a')

        if len(a) > 0 and a[0].attrs['title'] == 'Clique para expandir':
            comentario = a[0].get_text()
            # Execita a chamada que a página executaria caso o clique fosse realizado
            onclick = linha_documento.attrs['onclick']
            codigo_fornecedor = onclick[onclick.find('(') + 1:onclick.find(',')]
            url_documentos_fornecedores = url_base + f'18/Atas/DocumentoFornecedor/?ttCD_LICITACAO={chave}' \
                f'&ttCD_FORNECEDOR={codigo_fornecedor}'

            driver_documentos_fornecedor = __get_browser(url_documentos_fornecedores)
            html = driver_documentos_fornecedor.page_source
            driver_documentos_fornecedor.close()
            driver_documentos_fornecedor.quit()

            parser_documentos_fornecedor = BeautifulSoup(html, 'lxml')
            links = parser_documentos_fornecedor.findAll('a')

            for link in links:
                nome_arquivo = link.get_text().strip()
                if nome_arquivo != '':
                    linhas_df_documentos.append(
                        [codigo_licitacao, numero_processo_interno, nome_arquivo, '', url_base + link.attrs['href'],
                         comentario])
        else:
            linhas_df_documentos.append(
                [codigo_licitacao, numero_processo_interno, tds[0].get_text(), tds[1].get_text(),
                 tds[2].findAll('a')[0].attrs['href'], ''])

    return linhas_df_documentos, nomes_colunas_documentos


def processar_itens(codigo_licitacao, parser_detalhes):
    div_itens = parser_detalhes.findAll('div', {'class': 'list-itens-processo'})[0]
    divs_itens = div_itens.findAll('div', {'class': 'item-processo'})
    linhas_df_itens = []
    for div_item in divs_itens:
        spans = div_item.findAll('span')
        codigo_item = spans[0].contents[1].string
        descricao = spans[1].get_text()
        unidade = spans[2].contents[2].string
        quantidade = spans[3].contents[2].string
        melhor_lance = spans[4].contents[2].string
        svgs = spans[5].findAll('svg')

        try:
            situacao = svgs[0].get_text()
        except IndexError:
            print('aqui')

        modalidade = svgs[1].find('title').get_text()
        linhas_df_itens.append(
            [codigo_licitacao, codigo_item, descricao, unidade, quantidade, melhor_lance, situacao, modalidade])
    return linhas_df_itens


def __get_browser(url):
    chromeOptions = webdriver.ChromeOptions()
    chromeOptions.add_argument('--headless')

    locale.setlocale(locale.LC_ALL, "pt_br")

    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chromeOptions)
    driver.get(url)
    return driver


def __get_paginacao(soup):
    paginador = soup.findAll("ul", {"class": "pagination"})[0]
    paginas = paginador.findAll('li')
    proximas_paginas = []

    for pagina in paginas[1:]:
        link = pagina.findAll('a')[0].attrs['href']
        proximas_paginas.append(link)

    return proximas_paginas


#main()
