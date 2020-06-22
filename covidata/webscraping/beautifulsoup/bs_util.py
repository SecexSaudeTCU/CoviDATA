import requests
from bs4 import BeautifulSoup


def extrair_tabela(url, indice):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    tabela = soup.find_all('table')[indice]
    linhas = tabela.find_all('tr')
    titulos = linhas[0]
    titulos_colunas = titulos.find_all('td')
    colunas = [titulo_coluna.get_text() for titulo_coluna in titulos_colunas]
    linhas = linhas[1:]
    lista_linhas = []

    for linha in linhas:
        data = linha.find_all("td")
        nova_linha = [data[i].get_text() for i in range(len(colunas))]
        lista_linhas.append(nova_linha)

    return colunas, lista_linhas