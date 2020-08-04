from os import path

import os
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from spacy import displacy
from spacy.tokens.doc import Doc

from covidata import config


class NER(ABC):

    def extrair_entidades(self, df):
        diretorio_saida = os.path.join(config.diretorio_noticias, 'html')

        if not path.exists(diretorio_saida):
            os.makedirs(diretorio_saida)

        Doc.set_extension("entidades_originais", default=[])
        Doc.set_extension("entidades_relacionadas", default=[])

        for i in range(0, len(df)):
            texto = df.loc[i, 'texto']
            titulo = df.loc[i, 'title']
            midia = df.loc[i, 'media']
            data = df.loc[i, 'date']
            link = df.loc[i, 'link']
            self.__extrair_entidades_de_artigo(texto, i, titulo, midia, data, link, diretorio_saida)

        return diretorio_saida

    @abstractmethod
    def _extrair_entidades_de_texto(self, texto):
        pass

    def __extrair_entidades_de_artigo(self, texto, numero, titulo, midia, data, link, diretorio_saida):
        if type(texto) != float:
            doc = self._extrair_entidades_de_texto(texto)
            html = displacy.render(doc, style="ent")
            soup = BeautifulSoup(html)
            marks = soup.find_all('mark')

            if len(doc._.entidades_relacionadas) > 0:
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
