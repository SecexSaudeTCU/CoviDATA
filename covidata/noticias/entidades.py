import pt_core_news_sm
from bs4 import BeautifulSoup
from spacy import displacy
from spacy.tokens.doc import Doc

from covidata.noticias.contratados.identificacao_contratados import IdentificadorContratados
from covidata import config
import os
from os import path


def extrair_entidades(df):
    diretorio_saida = os.path.join(config.diretorio_noticias, 'html')

    if not path.exists(diretorio_saida):
        os.makedirs(diretorio_saida)

    identificador_contratados = IdentificadorContratados()
    Doc.set_extension("entidades_originais", default=[])
    Doc.set_extension("entidades_relacionadas", default=[])
    nlp = pt_core_news_sm.load()
    nlp.add_pipe(identificador_contratados, last=True)

    for i in range(0, len(df)):
        texto = df.loc[i, 'texto']
        titulo = df.loc[i, 'title']
        midia = df.loc[i, 'media']
        data = df.loc[i, 'date']
        link = df.loc[i, 'link']
        __extrair_entidades_de_artigo(nlp, texto, i, titulo, midia, data, link, diretorio_saida)

    return diretorio_saida


def __extrair_entidades_de_artigo(nlp, texto, numero, titulo, midia, data, link, diretorio_saida):
    if type(texto) != float:
        doc = nlp(texto)
        html = displacy.render(doc, style="ent")
        soup = BeautifulSoup(html)
        marks = soup.find_all('mark')

        for i, mark in enumerate(marks):
            mark['title'] = ''
            for entidade_relacionada in doc._.entidades_relacionadas[i]:
                mark['title'] += entidade_relacionada + '\n'

        cabecalho = '<p><b>Título: </b>' + titulo + '<br/>' + '<b>Mídia: </b>' + str(midia) + '<br/>' + \
                    '<b>Data: </b>' + data + '<br/>' + '<b>Link: </b><a href=' + link + '>' + link + '</a><br/></p>'
        soup.body.insert(0, BeautifulSoup(cabecalho))
        html = str(soup)

        with open(os.path.join(diretorio_saida, f"./{numero}.html"), 'w+', encoding="utf-8") as fp:
            fp.write(html)
            fp.close()
