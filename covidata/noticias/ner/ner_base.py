from abc import ABC, abstractmethod

import pandas as pd
from seqeval.metrics import accuracy_score
from seqeval.metrics import classification_report
from seqeval.metrics import f1_score

from covidata.noticias.contratados.identificacao_contratados import filtrar_contratados


class NER(ABC):

    def __init__(self, filtrar_contratados=False):
        self.filtrar_contratados = filtrar_contratados

    @abstractmethod
    def _get_map_labels(self):
        pass

    def extrair_entidades(self, df):
        resultado_analise = dict()
        for i in range(0, len(df)):
            texto = df.loc[i, 'texto']
            titulo = df.loc[i, 'title']
            midia = df.loc[i, 'media']

            if pd.isna(midia):
                midia = 'N/A'

            data = df.loc[i, 'date']
            link = df.loc[i, 'link']
            entidades_texto = self._extrair_entidades_de_texto(texto)

            if self.filtrar_contratados:
                entidades_texto = filtrar_contratados(entidades_texto)

            resultado_analise[(titulo, link, midia, data, texto)] = entidades_texto

        if self.filtrar_contratados:
            df = pd.concat(
                {k: pd.DataFrame(v, columns=['ENTIDADE', 'CLASSIFICAÇÃO', 'ENTIDADES RELACIONADAS']) for k, v in
                 resultado_analise.items()})
        else:
            df = pd.concat(
                {k: pd.DataFrame(v, columns=['ENTIDADE', 'CLASSIFICAÇÃO']) for k, v in resultado_analise.items()})
        return df

    @abstractmethod
    def _extrair_entidades_de_texto(self, texto):
        pass

    def avaliar(self):
        return ''

class Avaliacao():
    def __init__(self, y_true, y_pred):
        self.f1 = f1_score(y_true, y_pred)
        self.acuracia = accuracy_score(y_true, y_pred)
        self.relatorio_classificacao = classification_report(y_true, y_pred)

    def __str__(self):
        return f'f-1 = {self.f1}\n' + f'acurácia = {self.acuracia}\n' + 'Relatório = \n' + self.relatorio_classificacao
