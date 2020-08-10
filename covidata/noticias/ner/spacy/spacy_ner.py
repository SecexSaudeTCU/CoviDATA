import pandas as pd
import pt_core_news_sm

from covidata.noticias.ner.ner_base import NER


class SpacyNER(NER):
    def __init__(self, filtrar_contratados=False):
        super().__init__(filtrar_contratados)
        self.nlp = pt_core_news_sm.load()
        self.map_labels = {'MISC': 'MISCELÂNEA', 'LOC': 'LOCAL', 'ORG': 'ORGANIZAÇÃO', 'PER': 'PESSOA'}

    def _get_map_labels(self):
        return self.map_labels

    def _extrair_entidades_de_texto(self, texto):
        retorno = []
        if type(texto) != float:
            doc = self.nlp(texto)
            for ent in doc.ents:
                retorno.append((''.join(str(ent)), self._get_map_labels()[ent.label_]))

        return retorno


if __name__ == '__main__':
    df = pd.DataFrame()
    dados = [['', '', '', '',
              'O Tribunal de Contas da União é um órgão sediado em Brasília.  Sua sede fica na Esplanada dos ' \
              'Ministérios, portanto muito próxima à Catedral Metropolitana.']]
    df = pd.DataFrame(dados, columns=['title', 'media', 'date', 'link', 'texto'])
    SpacyNER().extrair_entidades(df)
