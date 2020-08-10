from covidata.noticias.ner.ner_base import NER
from polyglot.downloader import downloader
from polyglot.text import Text
import pandas as pd

class PolyglotNER(NER):
    def __init__(self, filtrar_contratados=False):
        super().__init__(filtrar_contratados)
        downloader.download("embeddings2.pt")
        downloader.download("ner2.pt")

        #Labels associadas a empresas/pessoas nos experimentos: LOCAL, ORGANIZAÇÃO E PESSOA.
        self.map_labels = {'I-LOC': 'LOCAL', 'I-ORG': 'ORGANIZAÇÃO', 'I-PER': 'PESSOA'}

    def _get_map_labels(self):
        return self.map_labels

    def _extrair_entidades_de_texto(self, texto):
        retorno = []
        if type(texto) != float and len(texto.strip()) > 0:
            doc = Text(texto, 'pt')

            for ent in doc.entities:
                retorno.append((' '.join(ent), self._get_map_labels()[ent.tag]))

        return retorno

if __name__ == '__main__':
    df = pd.DataFrame()
    dados = [['', '', '', '',
              'O Tribunal de Contas da União é um órgão sediado em Brasília.  Sua sede fica na Esplanada dos ' \
              'Ministérios, portanto muito próxima à Catedral Metropolitana.']]
    df = pd.DataFrame(dados, columns=['title', 'media', 'date', 'link', 'texto'])
    df_ner = PolyglotNER().extrair_entidades(df)
    df_ner.to_excel('polyglot_sample.xlsx')