from abc import ABC, abstractmethod
import pandas as pd

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

