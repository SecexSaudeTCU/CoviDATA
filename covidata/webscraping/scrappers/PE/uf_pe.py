#TODO: Falta Portal de transparência de PE

import requests
from bs4 import BeautifulSoup
import os
from os import path
import config


def download(url, diretorio, caminho_completo):
    if not path.exists(diretorio):
        os.makedirs(diretorio)

    r = requests.get(url)
    with open(caminho_completo, 'wb') as f:
        f.write(r.content)


def main():
    url = 'http://transparencia.recife.pe.gov.br/codigos/web/estaticos/estaticos.php?nat=COV#filho'
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    results = soup.find(id='treeview2')
    nivel1 = 'Processos de aquisições de bens e serviços oriundos de Dispensa ou Inexigibilidade'
    span = results.find_all('span', string=nivel1)
    ul_nivel2 = span[0].next_sibling

    setores = ['Gabinete de Projetos Especiais', 'Fundo Municipal de Assistência Social',
               'Fundo Municipal da Pessoa Idosa', 'Secretaria de Educação',
               'Secretaria de Governo e Participação Social', 'Secretaria de Infraestrutura',
               'Secretaria de Saúde']
    baixar_arquivos_em_tres_niveis(nivel1, ul_nivel2, nivel2='Com base na Lei nº 13.979/2020',
                                   niveis3=setores)

    baixar_arquivos_em_tres_niveis(nivel1, ul_nivel2, nivel2='Com base na Lei nº 8.666/ 1993',
                                   niveis3=['Secretaria de Saúde'])

    baixar_arquivos_em_dois_niveis(results, setores, nivel1='Publicações em Diário Oficial')

    temas = ['Matérias Televisivas', 'Dificuldades do Mercado PrIvado', 'Matérias Preços Abusivos',
             'Matérias Escassez de Insumos', 'Matérias Prazos de Entregas']

    baixar_arquivos_em_dois_niveis(results, temas, nivel1='Notícias sobre aquisições e contratações')


def baixar_arquivos_em_dois_niveis(results, titulos, nivel1='Publicações em Diário Oficial'):
    span = results.find_all('span', string=nivel1)
    ul_nivel2 = span[0].next_sibling

    for titulo in titulos:
        span = ul_nivel2.find_all('span', string=titulo)
        lista = span[0].next_sibling
        links = lista.find_all('a')
        baixar_arquivos_lista(links, nivel1, titulo, '')


def baixar_arquivos_em_tres_niveis(nivel1, ul_nivel2, nivel2, niveis3):
    span = ul_nivel2.find_all('span', string=nivel2)
    ul = span[0].next_sibling

    for nivel3 in niveis3:
        span = ul.find_all('span', string=nivel3)
        lista = span[0].next_sibling
        links = lista.find_all('a')
        baixar_arquivos_lista(links, nivel1, nivel2, nivel3)


def baixar_arquivos_lista(links, nivel1, nivel2, nivel3):
    arquivos = 0
    for link in links:
        url = link.attrs['href']
        arquivos += 1
        diretorio = path.join(config.diretorio_dados, 'PE', 'portal_transparencia', 'Recife', nivel1,
                              nivel2.replace('/', ' '), nivel3)[0:218].rstrip()
        extensao = url[url.rfind('.'):len(url)]
        nome_arquivo = 'arquivo' + str(arquivos) + extensao
        caminho_completo = os.path.join(diretorio, nome_arquivo)

        while len(caminho_completo) >= 218:
            qtd_caracteres_a_serem_removidos = len(caminho_completo) - 218
            diretorio = diretorio[0:len(diretorio) - qtd_caracteres_a_serem_removidos].rstrip()

            if os.path.exists(diretorio):
                arquivos_existentes = os.listdir(diretorio)
                while 'arquivo' + str(arquivos) + extensao in arquivos_existentes:
                    arquivos += 1
                    nome_arquivo = 'arquivo' + str(arquivos) + extensao

            caminho_completo = os.path.join(diretorio, nome_arquivo)

        download(url, diretorio, caminho_completo)

