import pt_core_news_sm
from spacy.tokens.doc import Doc

from covidata.noticias.contratados.identificacao_contratados import IdentificadorContratados
from covidata.noticias.ner.ner_base import NER
import pandas as pd


class SpacyNER(NER):
    def __init__(self):
        self.nlp = pt_core_news_sm.load()
        # self.nlp.add_pipe(IdentificadorContratados(), last=True)
        # Doc.set_extension("entidades_originais", default=[])
        # Doc.set_extension("entidades_relacionadas", default=[])
        self.map_labels = {'MISC': 'MISCELÂNEA', 'LOC': 'LOCAL', 'ORG': 'ORGANIZAÇÃO', 'PER': 'PESSOA'}

    def _get_map_labels(self):
        return self.map_labels

    def _extrair_entidades_de_texto(self, texto):
        retorno = []
        if type(texto) != float:
            doc = self.nlp(texto)
            for ent in doc.ents:
                retorno.append((ent.string, self._get_map_labels()[ent.label_]))

        return retorno


if __name__ == '__main__':
    df = pd.DataFrame()
    dados = [['', '', '', '',
              'O Tribunal de Contas da União é um órgão sediado em Brasília.  Sua sede fica na Esplanada dos ' \
              'Ministérios, portanto muito próxima à Catedral Metropolitana.']]
    df = pd.DataFrame(dados, columns=['title', 'media', 'date', 'link', 'texto'])
    SpacyNER().extrair_entidades(df)
