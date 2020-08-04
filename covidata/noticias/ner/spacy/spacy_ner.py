import pt_core_news_sm

from covidata.noticias.contratados.identificacao_contratados import IdentificadorContratados
from covidata.noticias.ner.ner_base import NER


class SpacyNER(NER):
    def __init__(self, df):
        super().__init__(df)
        self.nlp = pt_core_news_sm.load()
        self.nlp.add_pipe(IdentificadorContratados(), last=True)

    def _extrair_entidades_de_texto(self, texto):
        doc = self.nlp(texto)
        return doc
