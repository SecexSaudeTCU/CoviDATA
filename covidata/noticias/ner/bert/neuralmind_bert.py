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

        resultado_pos_processado = self.__pos_processar_resultados(resultado)

        return self._to_spacy_Doc(resultado_pos_processado, texto)

    def __pos_processar_resultados(self, resultado):
        # Unifica os tokens "quebrados" em tokens únicos, definindo a classificação de acordo com a classificação do
        # subtoken com score mais alto
        resultado_pos_processado = []
        melhor_score = 0
        melhor_classificacao = None
        sublista = None

        for item in resultado:
            word = item['word']

            if '#' not in word:
                # Adiciona os subtokens anteriormente encontrados que até agora não tinham sido adicionados, formando uma
                # nova palavra contígua.
                if sublista:
                    self.__adicionar_ultimas_palavras(melhor_classificacao, melhor_score, resultado_pos_processado,
                                                      sublista)
                    sublista = None

                item['index'] = len(resultado_pos_processado) + 1
                resultado_pos_processado.append(item)
            else:
                # Remove o último elemento da lista pós-processada
                if not sublista:
                    inicio = resultado_pos_processado.pop()
                    sublista = [inicio['word']]
                    melhor_score = inicio['score']
                    melhor_classificacao = inicio['entity']

                if item['score'] > melhor_score:
                    melhor_score = item['score']
                    melhor_classificacao = item['entity']

                sublista.append(word.replace('##', ''))
        # Adiciona os subtokens anteriormente encontrados que até agora não tinham sido adicionados, formando uma
        # nova palavra contígua.
        if sublista:
            self.__adicionar_ultimas_palavras(melhor_classificacao, melhor_score, resultado_pos_processado,
                                              sublista)
        return resultado_pos_processado

    def __adicionar_ultimas_palavras(self, melhor_classificacao, melhor_score, resultado_pos_processado, sublista):
        palavra = ''.join(sublista)
        novo_item = {'word': palavra, 'score': melhor_score, 'entity': melhor_classificacao,
                     'index': len(resultado_pos_processado) + 1}
        resultado_pos_processado.append(novo_item)

    def _to_spacy_Doc(self, resultado_pos_processado, texto):
        vocab_tokens = self.nlp.tokenizer.get_vocab().keys()
        tokens = []
        words = self.nlp.tokenizer.tokenize(texto)
        for word in words:
            if '#' not in word:
                tokens.append(word)
            else:
                tokens.append(tokens.pop() + word.replace('#', ''))
        doc = Doc(Vocab(strings=vocab_tokens), words=tokens)
        entidades = []
        offset = 0
        for item in resultado_pos_processado:
            word = item['word']

            if word in tokens:
                try:
                    start = tokens.index(word, offset)
                    span = Span(doc, label=(self.map_labels[item['entity']]), start=start, end=start + 1)
                    entidades.append(span)
                    offset = start
                except ValueError:
                    print(
                        'Erro na identificação de posição no texto.  Termo "' + word + '" não encontrado na lista de tokens.')

        doc.ents = tuple(entidades)

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
