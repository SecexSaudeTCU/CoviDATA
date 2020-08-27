import numpy as np
from transformers import pipeline

from covidata.noticias.ner.ner_base import NER


class BaseBERT_NER(NER):
    def __init__(self, filtrar_contratados=False):
        super().__init__(filtrar_contratados)
        self.nlp = pipeline("ner")
        self.map_labels = {'I-MISC': 'MISCELÂNEA', 'I-LOC': 'LOCAL', 'I-ORG': 'ORGANIZAÇÃO', 'I-PER': 'PESSOA'}

    def _get_map_labels(self):
        return self.map_labels

    def _extrair_entidades_de_texto(self, texto):
        if texto.strip() != '':
            # Respeitando o tamanho máximo de sequência para o qual o modelo foi originalmente treinado, quebra em
            # diferentes documentos quanto forem necessários.
            tokens = self.nlp.tokenizer.encode(texto)

            if len(tokens) > self.nlp.tokenizer.max_len:
                # Quebra o texto em textos menores.  Utiliza o número de caracteres como heurística, pois no pior caso o
                # tokenizador do BERT vai quebrar o texto em um token para cada caracter.
                textos = []
                palavras = texto.split()
                f = 0
                inicio_atual = 0

                for p in palavras:
                    i = texto.find(p, f)
                    f = i + len(p)
                    if f >= inicio_atual + self.nlp.tokenizer.max_len:
                        textos.append(texto[inicio_atual: i])
                        inicio_atual = i

                textos.append(texto[inicio_atual: len(texto)])
                retornos = []

                for subtexto in textos:
                    resultado, ids = self._extrair_entidades_originais(subtexto)
                    retornos += self._pos_processar(resultado, ids)

                return retornos
            else:
                resultado, ids = self._extrair_entidades_originais(texto)
                return self._pos_processar(resultado, ids)
        else:
            return []

    def _extrair_entidades_originais(self, texto):
        resultado = self.nlp(texto)

        # Recupera a lista de tokens (aqui, subwords)
        palavras = texto.split()
        ids = [self.nlp.tokenizer.encode(palavra) for palavra in palavras]

        return resultado, ids

    def _pos_processar(self, resultado, ids):
        lista_entidades = self.__agrupar_termos(ids, resultado)

        retorno = self.__agrupar_entidades(lista_entidades)
        return retorno

    def __agrupar_termos(self, ids, resultado):
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

            sublista_scores = scores[prefixo_atual:proximo_prefixo]
            indice_melhor_score = np.asarray(sublista_scores).argmax()
            melhor_classificacao = classificacoes[prefixo_atual:proximo_prefixo][indice_melhor_score]
            sublista_indices = indices[prefixo_atual:proximo_prefixo]
            indice_prefixo = sublista_indices[0]

            termo = self.__get_termo(ids, indice_prefixo)

            classificacao_entidade_atual = self._get_map_labels()[melhor_classificacao]
            lista_entidades.append((termo, classificacao_entidade_atual, sublista_indices))
        return lista_entidades

    def __agrupar_entidades(self, lista_entidades):
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
        novo_termo = ' '.join(t for t, _, _ in sublista if t)
        if entidade_anterior:
            retorno.append((novo_termo, entidade_anterior[1]))
        return retorno

    def __get_termo(self, ids, indice_prefixo):
        j = 0
        termo = None
        for palavra in ids:
            for token in palavra:
                if token != self.nlp.tokenizer.cls_token_id and token != self.nlp.tokenizer.sep_token_id:
                    if j == indice_prefixo - 1:
                        termo = self.nlp.tokenizer.decode(palavra[1:-1])
                        break
                    else:
                        j += 1
            if termo:
                break
        return termo
