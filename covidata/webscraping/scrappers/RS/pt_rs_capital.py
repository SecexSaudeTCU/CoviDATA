import time

import requests
from bs4 import BeautifulSoup

from covidata import config
import pandas as pd
import logging

import re
from urllib.parse import urlparse
from urllib.parse import parse_qs

from covidata.webscraping.selenium.selenium_util import get_browser
from covidata.persistencia.dao import persistir_dados_hierarquicos


def pt_PortoAlegre():
    url = config.url_pt_PortoAlegre
    parsed_uri = urlparse(url)
    url_base = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)

    page = requests.get(url)
    parser = BeautifulSoup(page.content, 'lxml')

    proximas_paginas = __get_paginacao(parser, url_base)

    nomes_colunas_documentos = []
    linhas_df = []
    linhas_df_documentos = []
    linhas_df_itens = []

    linhas_df_documentos, linhas_df_itens, nomes_colunas_documentos = __processar_pagina(linhas_df,
                                                                                         linhas_df_documentos,
                                                                                         linhas_df_itens,
                                                                                         nomes_colunas_documentos,
                                                                                         parser,
                                                                                         url_base)

    for pagina in proximas_paginas:
        parser = __parse(pagina)
        linhas_df_documentos, linhas_df_itens, nomes_colunas_documentos = __processar_pagina(linhas_df,
                                                                                             linhas_df_documentos,
                                                                                             linhas_df_itens,
                                                                                             nomes_colunas_documentos,
                                                                                             parser, url_base)

    nomes_colunas = ['Número da licitação', 'Objeto', 'Tipo', 'Status', 'Aplicar o Decreto 10.024/2019',
                     'Início de propostas', 'Final de propostas', 'Limite para impugnações', 'Data de Abertura',
                     'Pregoeiro', 'Autoridade Competente', 'Apoio', 'Origem dos Recursos', 'Operação']
    df = pd.DataFrame(linhas_df, columns=nomes_colunas)

    df_documentos = pd.DataFrame(linhas_df_documentos, columns=nomes_colunas_documentos)

    nomes_colunas_itens = ['Número da Licitação', 'Item', 'Descrição', 'Unidade', 'Quantidade', 'Melhor Lance (R$',
                           'Situação', 'Modalidade']
    df_itens = pd.DataFrame(linhas_df_itens, columns=nomes_colunas_itens)

    persistir_dados_hierarquicos(df, {'Documentos': df_documentos, 'Itens': df_itens}, 'portal_transparencia',
                                 'Licitacoes', 'RS', 'Porto Alegre')


def __get_paginacao(parser, url_base):
    tag_paginacao = parser.findAll("ul", {"class": "pagination"})

    if len(tag_paginacao) > 0:
        paginador = tag_paginacao[0]
        paginas = paginador.findAll('li')
        proximas_paginas = []

        for pagina in paginas[1:]:
            link = url_base + pagina.findAll('a')[0].attrs['href']
            proximas_paginas.append(link)

        return proximas_paginas
    else:
        return []


def __get_paginacao_itens(parser, url_base, codigo_interno_licitacao):
    tag_paginacao = parser.findAll("ul", {"class": "pagination"})

    if len(tag_paginacao) > 0:
        paginador = tag_paginacao[0]
        paginas = paginador.findAll('li')
        proximas_paginas = []
        numero_pagina = 2

        for _ in paginas[1:]:
            link = url_base + f'18/SessaoPublica/lances/htmlAba.asp?param=0&ttTipo=0&ttCD_LICITACAO=' \
                              f'{codigo_interno_licitacao}&ttPagina={numero_pagina}'
            numero_pagina += 1
            proximas_paginas.append(link)

        return proximas_paginas
    else:
        return []


def __processar_pagina(linhas_df, linhas_df_documentos, linhas_df_itens, nomes_colunas_documentos, parser, url_base):
    registros = parser.findAll('div', {'class': 'item-registro xs-col-12'})

    for registro in registros:
        textos = re.split('\r\n|\n\t|\t\n', registro.findAll('h2')[0].get_text().strip())

        numero_licitacao = textos[0]

        conteudo = registro.findAll('div')[0]
        divs = conteudo.findAll('div')
        div_descricao_registro = divs[0]
        objeto = div_descricao_registro.findAll('p')[0].get_text().strip()
        div_detalhes = divs[1]
        spans = div_detalhes.findAll('span')

        tipo = spans[0].get_text().strip()
        tipo = tipo[tipo.find('\n') + 1:len(tipo)].strip()

        status = spans[1].get_text().strip()
        status = status[status.find('\n') + 1:len(status)].strip()

        # página de detalhes
        parser_detalhes, codigo_interno_licitacao = __processar_detalhes(numero_licitacao, linhas_df, objeto, spans,
                                                                         status, tipo, url_base)

        # documentos
        linhas_df_documentos, nomes_colunas_documentos = __processar_documentos(numero_licitacao, linhas_df_documentos,
                                                                                parser_detalhes, url_base)

        # itens
        linhas_df_itens = __processar_itens(numero_licitacao, linhas_df_itens, parser_detalhes, url_base,
                                            codigo_interno_licitacao)

    return linhas_df_documentos, linhas_df_itens, nomes_colunas_documentos


def __processar_detalhes(numero_licitacao, linhas_df, objeto, spans, status, tipo, url_base):
    dados_adicionais = spans[2]
    url_detalhes = dados_adicionais.findAll('a', {'title': 'Dados do Processo'})[0].attrs['href']
    codigo_interno_licitacao = url_detalhes[url_detalhes.rfind('-') + 1:url_detalhes.rfind('/')]
    url_detalhes = url_base + url_detalhes

    parser_detalhes = __parse(url_detalhes)

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
    origem_recursos = ''
    operacao = ''

    i = 11
    if 'Origem dos Recursos:' in bs[i].get_text():
        origem_recursos = bs[i].next_sibling.string.strip()
        i += 1

    if 'Operação:' in bs[i].get_text():
        operacao = bs[i].next_sibling.string.strip()

    linhas_df.append([numero_licitacao, objeto, tipo, status, aplicar_decreto, inicio_propostas, final_propostas,
                      limite_impugnacoes, data_abertura, pregoeiro, autoridade_competente, apoio, origem_recursos,
                      operacao])

    return parser_detalhes, codigo_interno_licitacao


def __parse(url):
    driver = get_browser(url)
    html = driver.page_source
    driver.close()
    driver.quit()
    return BeautifulSoup(html, 'lxml')


def __processar_documentos(numero_licitacao, linhas_df_documentos, parser_detalhes, url_base):
    div_documentos = parser_detalhes.findAll('div', {'class': 'col-xs-12 col-sm-6 bgMidGreen'})[0]
    url_documentos = div_documentos.findAll('a', {'title': 'Atas e demais documentos do processo'})[0].attrs['href']
    url_documentos = url_base + url_documentos
    parsed_url_documentos = urlparse(url_documentos)
    chave = parse_qs(parsed_url_documentos.query)['ttCD_CHAVE'][0]

    parser_documentos = __parse(url_documentos)

    span = parser_documentos.findAll('span', {'class': 'green'})
    numero_processo_interno = span[0].findAll('b')[0].get_text()
    tabela = parser_documentos.findAll('table')[0]
    cabecalho = tabela.findAll('th')
    nomes_colunas_documentos = ['Número da Licitação', 'Número do Processo Interno'] + \
                               [coluna.get_text() for coluna in cabecalho] + \
                               ['Comentário']
    linhas_documentos = tabela.findAll('tbody')[0].findAll('tr')

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

            parser_documentos_fornecedor = __parse(url_documentos_fornecedores)

            links = parser_documentos_fornecedor.findAll('a')

            for link in links:
                nome_arquivo = link.get_text().strip()
                if nome_arquivo != '':
                    linhas_df_documentos.append(
                        [numero_licitacao, numero_processo_interno, nome_arquivo, '', url_base + link.attrs['href'],
                         comentario])
        else:
            linhas_df_documentos.append(
                [numero_licitacao, numero_processo_interno, tds[0].get_text(), tds[1].get_text(),
                 tds[2].findAll('a')[0].attrs['href'], ''])

    return linhas_df_documentos, nomes_colunas_documentos


def __processar_itens(numero_licitacao, linhas_df_itens, parser, url_base, codigo_interno_licitacao):
    proximas_paginas = __get_paginacao_itens(parser, url_base, codigo_interno_licitacao)

    __processar_pagina_itens(numero_licitacao, linhas_df_itens, parser)

    for pagina in proximas_paginas:
        parser = __parse(pagina)
        __processar_pagina_itens(numero_licitacao, linhas_df_itens, parser)

    return linhas_df_itens


def __processar_pagina_itens(numero_licitacao, linhas_df_itens, parser):
    div_itens = parser.findAll('div', {'class': 'list-itens-processo'})[0]
    divs_itens = div_itens.findAll('div', {'class': 'item-processo'})

    for div_item in divs_itens:
        spans = div_item.findAll('span')
        codigo_item = spans[0].contents[1].string
        descricao = spans[1].get_text()
        unidade = spans[2].contents[2].string
        quantidade = spans[3].contents[2].string
        melhor_lance = spans[4].contents[2].string
        svgs = spans[5].findAll('svg')

        situacao = ''
        modalidade = ''

        if len(svgs) > 0:
            situacao = svgs[0].get_text()
            modalidade = svgs[1].find('title').get_text()

        linhas_df_itens.append(
            [numero_licitacao, codigo_item, descricao, unidade, quantidade, melhor_lance, situacao, modalidade])


def main():
    logger = logging.getLogger('covidata')
    logger.info('Portal de transparência da capital...')
    start_time = time.time()

    pt_PortoAlegre()

    logger.info("--- %s segundos ---" % (time.time() - start_time))

