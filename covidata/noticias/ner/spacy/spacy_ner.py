import pickle
import random
import warnings

import jsonlines
import pt_core_news_sm
import spacy
from sklearn.model_selection import train_test_split
from spacy.gold import GoldParse
from spacy.gold import biluo_tags_from_offsets
from spacy.scorer import Scorer
from spacy.util import compounding, minibatch

from covidata import config
from covidata.noticias.ner.ner_base import NER, Avaliacao

# Mapeia os nomes das categorias de entidade usadas pela aplicação (ex.: base de treinamento rotulada) nos nomes
# de categoria correspondentes usados pelo Spacy.
map_app_cats_to_spacy_cats = {'PESSOA': 'PER', 'LOC': 'LOC', 'ORG': 'ORG', 'PUB': 'PUB'}


class SpacyNER(NER):
    def __init__(self, filtrar_contratados=False, modo_avaliacao=True, labels_validos=['PESSOA', 'LOC', 'ORG'],
                 diretorio_modelo_treinado=None, nome_algoritmo=None,
                 arquivo_validacao=None):
        super().__init__(filtrar_contratados, nome_algoritmo)

        # Caso tenha sido passado como parâmetro o diretório onde se encontra um modelo treinado (cujo fine tunning foi
        # feito para uma base de treinamento específica).
        if diretorio_modelo_treinado:
            self.nlp = spacy.load(diretorio_modelo_treinado)
        else:
            # Caso contrário, carrega o modelo default pré-treinado para Português.
            self.nlp = pt_core_news_sm.load()

        # Mapeia os nomes das categorias de entidade usados pelo Spacy em nomes úteis ao usuário final
        self.map_spacy_cats_to_user_cats = {'MISC': 'MISCELÂNEA', 'LOC': 'LOCAL', 'ORG': 'ORGANIZAÇÃO', 'PER': 'PESSOA',
                                            'PUB': 'INSTITUIÇÃO PÚBLICA'}
        self.modo_avalicao = modo_avaliacao
        self.preds = []
        self.labels_validos = labels_validos
        self.arquivo_validaco = arquivo_validacao

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

        with jsonlines.open(self.arquivo_validaco) as reader:
            for obj in reader:
                texto = obj['text']
                labels = obj['labels']
                doc_gold_text = self.nlp.make_doc(texto)
                text_entities = []

                for entity in labels:
                    if entity[2] in self.labels_validos:
                        # Traduz no nome de categoria correspondente adotado pelo Spacy.
                        entity[2] = map_app_cats_to_spacy_cats[entity[2]]
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
        return super().__str__() + '\nAvaliação a nível de tokens:\n' + f'f-1 = {self.scorer.ents_f}\n' + \
               f'precisão = {self.scorer.ents_p}\n' + f'recall = {self.scorer.ents_r}\n'\
               #TODO: Esta saída está reportando resultados inconsistentes.
               #+ 'Por tipo de entidade:\n' + \
               #str(self.scorer.scores['ents_per_type'])


def criar_base_treinamento_validacao():
    dados_treinamento = []

    with jsonlines.open(config.arquivo_noticias_rotulado) as reader:
        for obj in reader:
            texto = obj['text']
            labels = obj['labels']
            entidades = []

            for label in labels:
                inicio = label[0]
                fim = label[1]
                categoria = label[2]
                entidades.append((inicio, fim, map_app_cats_to_spacy_cats[categoria]))

            dados_treinamento.append((texto, {'entities': entidades}))

    with open('base_spacy.pickle', 'wb') as fp:
        pickle.dump(dados_treinamento, fp)


def treinar():
    random.seed(0)
    nlp = pt_core_news_sm.load()

    if 'ner' not in nlp.pipe_names:
        ner = nlp.create_pipe('ner')
        nlp.add_pipe(ner)
    else:
        ner = nlp.get_pipe('ner')

    # Adiciona o novo label "PUB"
    ner.add_label('PUB')

    optimizer = nlp.resume_training()
    move_names = list(ner.move_names)
    n_iter = 30

    # Desabilita ouros pipelines para treinar apenas NER
    other_pipes = [pipe for pipe in nlp.pipe_names if pipe != 'ner']

    TESTING_DATA, TRAIN_DATA = get_bases_treinamento_validacao()

    with nlp.disable_pipes(*other_pipes):
        # show warnings for misaligned entity spans once
        warnings.filterwarnings("once", category=UserWarning, module='spacy')

        sizes = compounding(1.0, 4.0, 1.001)
        # batch up the examples using spaCy's minibatch
        for itn in range(n_iter):
            random.shuffle(TRAIN_DATA)
            batches = minibatch(TRAIN_DATA, size=sizes)
            losses = {}
            for batch in batches:
                texts, annotations = zip(*batch)
                nlp.update(texts, annotations, sgd=optimizer, drop=0.35, losses=losses)
            print("Losses", losses)

    # save model to output directory
    nlp.meta["name"] = 'spacy_PUB'  # rename model
    nlp.to_disk('.')

    # test the saved model
    nlp2 = spacy.load('.')
    # Check the classes have loaded back consistently
    assert nlp2.get_pipe("ner").move_names == move_names

    for texto, _ in TESTING_DATA:
        doc2 = nlp2(texto)
        for ent in doc2.ents:
            print(ent.label_, ent.text)


def get_bases_treinamento_validacao():
    with open('base_spacy.pickle', 'rb') as pickle_file:
        TOTAL_DATA = pickle.load(pickle_file)
    TRAIN_DATA, TESTING_DATA = train_test_split(TOTAL_DATA, test_size=0.3, random_state=42)
    return TESTING_DATA, TRAIN_DATA


def criar_base_validacao():
    validacao, _ = get_bases_treinamento_validacao()
    textos_validacao = set()

    for texto, _ in validacao:
        textos_validacao.add(texto.strip())

    linhas_val = []
    with jsonlines.open(config.arquivo_noticias_rotulado) as reader:
        for obj in reader:
            if obj['text'] in textos_validacao:
                linhas_val.append(obj)

    linhas_val = sorted(linhas_val, key=lambda i: i['text'])

    with jsonlines.open('json_val.jsonl', 'w') as writer:
        writer.write_all(linhas_val)


if __name__ == '__main__':
    # criar_base_treinamento()
    # treinar()
    criar_base_validacao()
