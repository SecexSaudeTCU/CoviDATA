import torch
from spacy.tokens.doc import Doc
from spacy.tokens.span import Span
from spacy.vocab import Vocab
from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline

from covidata.noticias.ner.ner_base import NER


class BaseBERT_NER(NER):
    def __init__(self):
        self.nlp = pipeline("ner")
        self.map_labels = {'I-MISC': 'MISC', 'I-LOC': 'LOC', 'I-ORG': 'ORG', 'I-PER': 'PER'}

    def _extrair_entidades_de_texto(self, texto):
        resultado = self.nlp(texto)
        return self._to_spacy_Doc(resultado, texto)

    #TODO: Remover este acoplamento em relação ao Spacy/Displacy e retornar o resultado na forma de planilha, por exemplo.
    def _to_spacy_Doc(self, resultado, texto):
        vocab_tokens = self.nlp.tokenizer.get_vocab().keys()
        words = self.nlp.tokenizer.tokenize(texto)
        doc = Doc(Vocab(strings=vocab_tokens), words=words)
        entidades = []

        for item in resultado:
            word = item['word']
            if word != '[CLS]':
                span = Span(doc, label=(self.map_labels[item['entity']]), start=item['index'] - 1, end=item['index'])
                entidades.append(span)

        doc.ents = tuple(set(entidades))

        return doc


class Neuralmind_PT_BERT_NER(NER):
    def extrair_entidades(self):
        for i in range(0, len(self.df)):
            texto = self.df.loc[i, 'texto']
            tokens = self.tokenizer.tokenize(self.tokenizer.decode(self.tokenizer.encode(texto)))
            inputs = self.tokenizer.encode(texto, return_tensors="pt")
            outputs = self.model(inputs)[0]
            predictions = torch.argmax(outputs, dim=2)
            label_list = ['LABEL_0', 'LABEL_1']
            print([(token, label_list[prediction]) for token, prediction in zip(tokens, predictions[0].numpy())])


class Neuralmind_PT_BaseBERT_NER(Neuralmind_PT_BERT_NER):
    def __init__(self, df):
        self.tokenizer = AutoTokenizer.from_pretrained('neuralmind/bert-base-portuguese-cased')
        self.model = AutoModelForTokenClassification.from_pretrained('neuralmind/bert-base-portuguese-cased')
        self.nlp = pipeline('ner', model=self.model, tokenizer=self.tokenizer)
        self.df = df


class Neuralmind_PT_LargeBERT_NER(Neuralmind_PT_BERT_NER):
    def __init__(self, df):
        self.tokenizer = AutoTokenizer.from_pretrained('neuralmind/bert-large-portuguese-cased')
        self.model = AutoModelForTokenClassification.from_pretrained('neuralmind/bert-large-portuguese-cased')
        self.nlp = pipeline('ner', model=self.model, tokenizer=self.tokenizer)
        self.df = df


"""
if __name__ == '__main__':
    df = pd.DataFrame()
    dados = [['O Tribunal de Contas da União é um órgão sediado em Brasília.  Sua sede fica na Esplanada dos ' \
              'Ministérios, portanto muito próxima à Catedral Metropolitana.']]
    df = pd.DataFrame(dados, columns=['texto'])
    BaseBERT_NER(df).extrair_entidades()
"""
