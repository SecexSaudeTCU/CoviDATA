from abc import ABC, abstractmethod
import pandas as pd

class NER(ABC):

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
            resultado_analise[(titulo, link, midia, data, texto)] = entidades_texto

        df = pd.concat(
            {k: pd.DataFrame(v, columns=['ENTIDADE', 'CLASSIFICAÇÃO']) for k, v in
             resultado_analise.items()})
        return df

    @abstractmethod
    def _extrair_entidades_de_texto(self, texto):
        pass

