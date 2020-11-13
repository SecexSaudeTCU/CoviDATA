import os
from os import path

import numpy as np
import pandas as pd

from covidata import config

# Coluna que indica se o CNPJ foi inferido a posteriori, por meio de consulta à base da Receita Federal (SIM), ou se já
# estava presente nos dados (NÃO).  Alternativamente, pode assumir o valor VER ABA CNPJs caso tenha sido encontrado mais
# de um CNPJ associado à razão social ou nome fantasia da empresa.
CNPJ_INFERIDO = 'CNPJ_INFERIDO'

ANO_PADRAO = 2020

"""
TABELA 1 – Informações relacionadas às despesas nos estados e municípios
"""
####################
## Dados principais
####################

# Descrição da fonte de extração dos dados.
FONTE_DADOS = 'FONTE_DADOS'

# Registro da data de extração dos dados.
DATA_EXTRACAO_DADOS = 'DATA_EXTRACAO_DADOS'

# UF da Unidade Gestora.
UF = 'UF'

# F- Federal; E- Estadual; e M- Municipal.
ESFERA = 'ESFERA'

ESFERA_FEDERAL = 'F'
ESFERA_ESTADUAL = 'E'
ESFERA_MUNICIPAL = 'M'

TIPO_FONTE_TCE = 'TCE'
TIPO_FONTE_TCM = 'TCM'
TIPO_FONTE_PORTAL_TRANSPARENCIA = 'Portal de Transparência'

# Código do Município (IBGE).
COD_IBGE_MUNICIPIO = 'COD_IBGE_MUNICIPIO'

# Nome do município
MUNICIPIO_DESCRICAO = 'MUNICIPIO_DESCRICAO'

# CNPJ do contratante.
CONTRATANTE_CNPJ = 'CONTRATANTE_CNPJ'

# Descrição do nome do Órgão.
CONTRATANTE_DESCRICAO = 'CONTRATANTE_DESCRICAO'

# Código do favorecido, CNPJ ou CPF. (Campo chave).
CONTRATADO_CNPJ = 'CONTRATADO_CNPJ'

# Descrição do favorecido do empenho.
CONTRATADO_DESCRICAO = 'CONTRATADO_DESCRICAO'

VALOR_CONTRATO = 'VALOR_R$'

# Objeto da contratação
DESPESA_DESCRICAO = 'DESPESA_DESCRICAO'

# TODO: Quais são os valores possíveis?
TIPO_DOCUMENTO = 'TIPO_DOCUMENTO'

# Número do Empenho (UG + Gestão + Empenho) (Campo chave).
DOCUMENTO_NUMERO = 'EMPENHO_NUMERO'

DOCUMENTO_DATA = 'DOCUMENTO_DATA'

ORIGEM_DADOS = 'ORIGEM_DADOS'

UF_ARQUIVO_ORIGEM = 'UF_ARQUIVO_ORIGEM'

DATA_CARGA = 'DATA_CARGA'


def consolidar_layout(df_original, dicionario_dados, esfera, fonte_dados, uf, codigo_municipio_ibge,
                      data_extracao, funcao_posprocessamento=None):
    """
    Consolida um conjunto de informações no formato padronizado, convetendo um dataframe de um formato em outro, em
    termos de oolunas.

    :param df_original: O dataframe original.
    :param dicionario_dados: Dicionário que mapeia nomes de colunas no dataframe original nos respectivos nomes de
        coluna no formato padronizado.
    :param uf: Sigla da unidade da federação.
    :param codigo_municipio_ibge: Código do município do IBGE, se aplicável.
    :param fonte_dados: Fonte dos dados (ex.: TCE | TCM | Portal de Transparência - <URL para a fonte dos dados>)
    :param esfera: Esfera administrativa (F- Federal; E- Estadual; e M- Municipal).
    :param data_extracao Data/hora em que os dados foram extraídos.
    :param funcao_posprocessamento: Callback para função que complementa a conversão.
    :return: df: Dataframe resultante.
    """
    df = __converter_dataframes(df_original, dicionario_dados, uf, codigo_municipio_ibge, fonte_dados, esfera,
                                data_extracao)
    if funcao_posprocessamento:
        df = funcao_posprocessamento(df)

    # Remove espaços extras do início e do final das colunas do tipo string
    # df_obj = df.select_dtypes(['object'])
    # df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())

    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    return df


def __converter_dataframes(df_original, dicionario_dados, uf, codigo_municipio_ibge, fonte_dados,
                           esfera, data_extracao):
    df = pd.DataFrame(columns=[FONTE_DADOS, DATA_EXTRACAO_DADOS, ESFERA, UF, COD_IBGE_MUNICIPIO, MUNICIPIO_DESCRICAO])

    for coluna_padronizada, coluna_correspondente in dicionario_dados.items():
        df[coluna_padronizada] = df_original.get(coluna_correspondente, np.nan)

    df[FONTE_DADOS] = fonte_dados
    df[DATA_EXTRACAO_DADOS] = data_extracao
    df[UF] = uf
    df[COD_IBGE_MUNICIPIO] = codigo_municipio_ibge
    df[ESFERA] = esfera

    return df


def salvar(df, uf, nome=''):
    """
    Salva um dataframe.

    :param df O dataframe a ser salvo.
    :param uf: A unidade da federação à qual o dataframe se refere.
    :param nome: Nome (opcional) que permite identificar o tipo de informação à qual o dataframe se refere.
    """
    diretorio = path.join(config.diretorio_dados, 'consolidados', uf)

    if not path.exists(diretorio):
        os.makedirs(diretorio)

    df.to_excel(path.join(diretorio, uf + nome + '.xlsx'), index=False)
