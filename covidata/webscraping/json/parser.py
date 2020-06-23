import json
import os
from abc import ABC, abstractmethod
from os import path

import pandas as pd

from covidata import config
from covidata.persistencia.dao import persistir_dados_hierarquicos
from covidata.webscraping.downloader import FileDownloader

class JSONParser(ABC):
    def __init__(self, url, campo_chave, nome_dados, fonte, uf, cidade=''):
        self.url = url
        self.diretorio = os.path.join(config.diretorio_dados, uf, fonte, cidade)
        self.campo_chave = campo_chave
        self.nome_dados = nome_dados
        self.fonte = fonte
        self.uf = uf
        self.cidade = cidade

    def parse(self):
        nome_arquivo = self.nome_dados + '.json'
        downloader = FileDownloader(self.diretorio, self.url, nome_arquivo)
        downloader.download()

        with open(os.path.join(downloader.diretorio_dados, downloader.nome_arquivo)) as json_file:
            dados = json.load(json_file)
            conteudo = self.json_parse(dados)

            colunas_df_principal = []
            linhas_df_principal = []

            dfs_auxiliares = dict()

            for elemento in conteudo:
                id = self.campo_chave
                valor_id = elemento[id]
                linha = []

                for key in elemento.keys():
                    if isinstance(elemento[key], list):
                        # cria um dataframe/aba com os elementos da lista
                        if len(elemento[key]) > 0:
                            lista_auxiliar = elemento[key]
                            colunas_df_auxiliar = [id]
                            linhas_df_auxiliar = []

                            for elemento_auxiliar in lista_auxiliar:
                                linha_df_auxiliar = [valor_id]

                                for key2 in elemento_auxiliar.keys():
                                    self.__processar_linha_univalorada(colunas_df_auxiliar, elemento_auxiliar, key2,
                                                                       linha_df_auxiliar, key)

                                linhas_df_auxiliar.append(linha_df_auxiliar)

                            df_auxiliar = pd.DataFrame(linhas_df_auxiliar, columns=colunas_df_auxiliar)
                            if not key in dfs_auxiliares:
                                dfs_auxiliares[key] = df_auxiliar
                            else:
                                dfs_auxiliares[key] = dfs_auxiliares[key].append(df_auxiliar)

                    else:
                        self.__processar_linha_univalorada(colunas_df_principal, elemento, key, linha)

                linhas_df_principal.append(linha)

            df_principal = pd.DataFrame(linhas_df_principal, columns=colunas_df_principal)

            persistir_dados_hierarquicos(df_principal, dfs_auxiliares, self.fonte, self.nome_dados, self.uf,
                                         self.cidade)

    def __processar_e_salvar_json(self, diretorio, url):
        if not path.exists(self.diretorio):
            os.makedirs(self.diretorio)

        conteudo = url.read().decode()
        data = self.json_parse(conteudo)

        with open(os.path.join(diretorio, self.nome_dados, '.json'), 'w') as f:
            f.write(conteudo)

        return data

    @abstractmethod
    def json_parse(self, conteudo):
        pass

    def __processar_linha_univalorada(self, colunas_df, elemento, key, linha, agrupador=None):
        if isinstance(elemento[key], dict):
            dicionario = elemento[key]
            for key2 in dicionario.keys():
                nome_coluna = key + '_' + key2
                if not nome_coluna in colunas_df:
                    colunas_df.append(nome_coluna)
                linha.append(dicionario[key2])
        else:
            if not key in colunas_df:
                colunas_df.append(key)
            elif agrupador and key == colunas_df[0]:
                # Neste caso, houve um coincidência de nome com o atributo-chave do elemento-pai - basta renomear a coluna.
                coluna_renomeada = agrupador + '_' + key
                if not coluna_renomeada in colunas_df:
                    colunas_df.append(coluna_renomeada)

            linha.append(elemento[key])
