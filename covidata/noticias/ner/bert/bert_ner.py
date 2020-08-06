import numpy as np
import torch
from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline

from covidata.noticias.ner.ner_base import NER


class BaseBERT_NER(NER):
    def __init__(self):
        self.nlp = pipeline("ner")
        self.map_labels = {'I-MISC': 'MISCELÂNEA', 'I-LOC': 'LOCAL', 'I-ORG': 'ORGANIZAÇÃO', 'I-PER': 'PESSOA'}

    def _extrair_entidades_de_texto(self, texto):
        resultado = self.nlp(texto)
        indices_prefixos = []
        classificacoes = []
        scores = []
        entidades = []
        lista_entidades = []
        indices = []

        # Pós-processa para juntar subtokens dentro da mesma palavra
        for i, item in enumerate(resultado):
            word = item['word']
            entidades.append(word)
            classificacoes.append(item['entity'])
            scores.append(item['score'])
            indices.append(item['index'])

            if '##' not in word:
                indices_prefixos.append(i)

        for i in range(len(indices_prefixos)):
            prefixo_atual = indices_prefixos[i]

            if i < len(indices_prefixos) - 1:
                proximo_prefixo = indices_prefixos[i + 1]
            else:
                proximo_prefixo = len(entidades)

            sublista_entidades = entidades[prefixo_atual:proximo_prefixo]
            termo = ''.join(sublista_entidades).replace('##', '')
            sublista_scores = scores[prefixo_atual:proximo_prefixo]
            indice_melhor_score = np.asarray(sublista_scores).argmax()
            melhor_classificacao = classificacoes[prefixo_atual:proximo_prefixo][indice_melhor_score]
            sublista_indices = indices[prefixo_atual:proximo_prefixo]
            classificacao_entidade_atual = self.map_labels[melhor_classificacao]
            lista_entidades.append((termo, classificacao_entidade_atual, sublista_indices))

        entidade_anterior = None
        indice_nova_entidade = 0
        retorno = []

        for i, ent in enumerate(lista_entidades):
            classificacao_entidade_atual = ent[1]
            inicio_entidade_atual = ent[2][0]

            if (entidade_anterior and (
                    classificacao_entidade_atual != entidade_anterior[1] or inicio_entidade_atual >
                    entidade_anterior[2][-1] + 1)):
                # nova entidade
                sublista = lista_entidades[indice_nova_entidade:i]
                novo_termo = ' '.join(t for t, _, _ in sublista)
                retorno.append((novo_termo, entidade_anterior[1]))
                indice_nova_entidade = i

            entidade_anterior = ent

        # Acrescenta a última entidade
        sublista = lista_entidades[indice_nova_entidade:len(lista_entidades)]
        novo_termo = ' '.join(t for t, _, _ in sublista)

        if entidade_anterior:
            retorno.append((novo_termo, entidade_anterior[1]))

        return retorno


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
    dados = [['', '', '', '',
              'O Tribunal de Contas da União é um órgão sediado em Brasília.  Sua sede fica na Esplanada dos ' \
              'Ministérios, portanto muito próxima à Catedral Metropolitana.']]
    df = pd.DataFrame(dados, columns=['title', 'media', 'date', 'link', 'texto'])
    BaseBERT_NER().extrair_entidades(df)
"""
