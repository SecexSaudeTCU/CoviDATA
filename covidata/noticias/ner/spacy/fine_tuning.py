import pickle
import random
import warnings

import jsonlines
import pt_core_news_sm
import spacy
from sklearn.model_selection import train_test_split
from spacy.util import compounding, minibatch

from covidata import config
from covidata.noticias.ner.spacy.spacy_ner import map_app_cats_to_spacy_cats


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
