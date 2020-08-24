import jsonlines
import pandas as pd
import pt_core_news_sm
from spacy.gold import GoldParse
from spacy.scorer import Scorer

from covidata.noticias.ner.ner_base import NER, Avaliacao
from spacy.gold import biluo_tags_from_offsets


class SpacyNER(NER):
    def __init__(self, filtrar_contratados=False, modo_avaliacao=True, labels_validos=['PESSOA', 'LOC', 'ORG']):
        super().__init__(filtrar_contratados)
        self.nlp = pt_core_news_sm.load()

        # Mapeia os nomes das categorias de entidade usados pelo Spacy em nomes úteis ao usuário final
        self.map_spacy_cats_to_user_cats = {'MISC': 'MISCELÂNEA', 'LOC': 'LOCAL', 'ORG': 'ORGANIZAÇÃO', 'PER': 'PESSOA'}

        # Mapeia os nomes das categorias de entidade usadas pela aplicação (ex.: base de treinamento rotulada) nos nomes
        # de categoria correspondentes usados pelo Spacy.
        self.map_app_cats_to_spacy_cats = {'PESSOA': 'PER', 'LOC': 'LOC', 'ORG': 'ORG'}

        self.modo_avalicao = modo_avaliacao
        self.preds = []
        self.textos = []
        self.labels_validos = labels_validos

    def _get_map_labels(self):
        return self.map_spacy_cats_to_user_cats

    def _extrair_entidades_de_texto(self, texto):
        retorno = []
        if type(texto) != float:
            doc = self.nlp(texto)

            # Salva o resultado para futura medição de desempenho
            if self.modo_avalicao:
                self.preds.append(doc)

            for ent in doc.ents:
                retorno.append((''.join(str(ent)), self._get_map_labels()[ent.label_]))

        return retorno

    def avaliar(self):
        i = 0
        scorer = Scorer()
        y_true = []
        y_pred = []

        with jsonlines.open('labeled_4_labels.jsonl') as reader:
            for obj in reader:
                texto = obj['text']
                labels = obj['labels']
                doc_gold_text = self.nlp.make_doc(texto)
                text_entities = []

                for entity in labels:
                    if entity[2] in self.labels_validos:
                        # Traduz no nome de categoria correspondente adotado pelo Spacy.
                        entity[2] = self.map_app_cats_to_spacy_cats[entity[2]]
                        text_entities.append(entity)

                # Avaliação a nível de tokens
                gold = GoldParse(doc_gold_text, entities=text_entities)
                pred_value = self.preds[i]
                scorer.score(pred_value, gold)

                # Avaliação a nível de entidades
                y_true.append(gold.ner)
                ents_list = list(pred_value.ents)
                ents_list = [(span.start_char, span.end_char, span.label_) for span in ents_list]
                tags = biluo_tags_from_offsets(self.nlp.make_doc(pred_value.text), ents_list)
                y_pred.append(tags)
                i += 1

        return AvaliacaoSpacy(y_true, y_pred, scorer)


class AvaliacaoSpacy(Avaliacao):
    def __init__(self, y_true, y_pred, scorer):
        super(AvaliacaoSpacy, self).__init__(y_true, y_pred)
        self.scorer = scorer

    def __str__(self):
        return super().__str__() + '\nAvaliação de nível de tokens:\n' + f'f-1 = {self.scorer.ents_f}\n' + \
               f'precisão = {self.scorer.ents_p}\n' + f'recall = {self.scorer.ents_r}\n' + 'Por tipo de entidade:\n' + \
               str(self.scorer.scores['ents_per_type'])


if __name__ == '__main__':
    df = pd.DataFrame()
    dados = [['', '', '', '',
              'O Tribunal de Contas da União é um órgão sediado em Brasília.  Sua sede fica na Esplanada dos ' \
              'Ministérios, portanto muito próxima à Catedral Metropolitana.']]
    df = pd.DataFrame(dados, columns=['title', 'media', 'date', 'link', 'texto'])
    SpacyNER().extrair_entidades(df)
