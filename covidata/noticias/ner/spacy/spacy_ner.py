import jsonlines
import pandas as pd
import pt_core_news_sm
from spacy.gold import GoldParse

from covidata.noticias.ner.ner_base import NER


class SpacyNER(NER):
    def __init__(self, filtrar_contratados=False, modo_avaliacao=True):
        super().__init__(filtrar_contratados)
        self.nlp = pt_core_news_sm.load()
        self.map_labels = {'MISC': 'MISCELÂNEA', 'LOC': 'LOCAL', 'ORG': 'ORGANIZAÇÃO', 'PER': 'PESSOA'}
        self.modo_avalicao = modo_avaliacao
        self.y_preds = []
        self.textos = []

    def _get_map_labels(self):
        return self.map_labels

    def _extrair_entidades_de_texto(self, texto):
        retorno = []
        if type(texto) != float:
            doc = self.nlp(texto)

            # Salva o resultado para futura medição de desempenho
            if self.modo_avalicao:
                self.y_preds.append(doc.ents)
                self.textos.append(texto)

            for ent in doc.ents:
                retorno.append((''.join(str(ent)), self._get_map_labels()[ent.label_]))

        return retorno

    def avaliar(self):
        self.calcular_y_true()
        print(self.y_true)

    def calcular_y_true(self):
        # Recupera os labels verdadeiros para posterior avaliação de desempenho dos modelos.
        self.y_true = []
        self.golds = []

        with jsonlines.open('labeled_4_labels.jsonl') as reader:
            for obj in reader:
                doc = self.nlp.tokenizer(obj['text'])
                labels = obj['labels']
                y_true_obj = []

                for token in doc:
                    inicio = token.idx
                    fim = inicio + len(token)
                    tipo = 'O'

                    for label in labels:
                        inicio_label = label[0]
                        fim_label = label[1]
                        cat = label[2]

                        if inicio >= inicio_label and fim <= fim_label:
                            tipo = cat
                            break

                    #y_true_obj.append((tipo, token))
                    y_true_obj.append(tipo)

                self.y_true.append(y_true_obj)
                self.golds.append(GoldParse(doc, entities=y_true_obj))



if __name__ == '__main__':
    df = pd.DataFrame()
    dados = [['', '', '', '',
              'O Tribunal de Contas da União é um órgão sediado em Brasília.  Sua sede fica na Esplanada dos ' \
              'Ministérios, portanto muito próxima à Catedral Metropolitana.']]
    df = pd.DataFrame(dados, columns=['title', 'media', 'date', 'link', 'texto'])
    SpacyNER().extrair_entidades(df)
