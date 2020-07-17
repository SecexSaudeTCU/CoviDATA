import os
from os import path

import numpy as np
import pandas as pd

from covidata import config

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

########################
## Dados secundários
########################

# Ano da execução da despesa.
ANO = 'ANO'

# Código da Fonte dos Recursos.
FONTE_RECURSOS_COD = 'FONTE_RECURSOS_COD'

# Descrição da Fonte dos Recursos.
FONTE_RECURSOS_DESCRICAO = 'FONTE_RECURSOS_DESCRICAO'

# Código numérico do Órgão.
ORGAO_COD = 'ORGAO_COD'

# Código numérico da Unidade Gestora.
UG_COD = 'UG_COD'

# Código de CNPJ da Unidade Gestora. (Campo chave).
UG_CNPJ = 'UG_CNPJ'

# Descrição do Nome da Unidade Gestora.
UG_DESCRICAO = 'UG_DESCRICAO'

# Código Funcional completo (função.Subfunção.Programa. Ação.Subtítulo).
FUNCIONAL = 'FUNCIONAL'

# Código da Função.
FUNCAO_COD = 'FUNCAO_COD'

# Descrição da Função.
FUNCAO_DESCRICAO = 'FUNCAO_DESCRICAO'

# Código da Subfunção.
SUBFUNCAO_COD = 'SUBFUNCAO_COD'

# Descrição da Subfunção.
SUBFUNCAO_DESCRICAO = 'SUBFUNCAO_DESCRICAO'

# Código do Programa.
PROGRAMA_COD = 'PROGRAMA_COD'

# Descrição do Programa.
PROGRAMA_DESCRICAO = 'PROGRAMA_DESCRICAO'

# Código da Ação.
ACAO_COD = 'ACAO_COD'

# Descrição da Ação.
ACAO_DESCRICAO = 'ACAO_DESCRICAO'

# Código do Subtítulo.
SUBTITULO_COD = 'SUBTITULO_COD'

# Descrição do Subtítulo.
SUBTITULO_DESCRICAO = 'SUBTITULO_DESCRICAO'

# Código da Modalidade de Aplicação.
MOD_APLICACAO_COD = 'MOD_APLICACAO_COD'

# Descrição da Modalidade de Aplicação.
# TODO: Pode significar modalidade de licitação?
MOD_APLIC_DESCRICAO = 'MOD_APLIC_DESCRICAO'

# Código do Grupo de Natureza de Despesa.
GND_COD = 'GND_COD'

# Descrição do Grupo de Natureza de Despesa.
GND_DESCRICAO = 'GND_DESCRICAO'

# Código do Elemento de Despesa.
ELEMENTO_DESPESA_COD = 'ELEMENTO_DESPESA_COD'

# Descrição do Elemento de Despesa.
ELEMENTO_DESPESA_DESCRICAO = 'ELEMENTO_DESPESA_DESCRICAO'

# Código do Subelemento de despesa.
SUB_ELEMENTO_DESPESA_COD = 'SUB_ELEMENTO_DESPESA_COD'

# Descrição do Subelemento de despesa.
SUB_ELEMENTO_DESPESA_DESCRICAO = 'SUB_ELEMENTO_DESPESA_DESCRICAO'

# Descrição da observação do empenho.
EMPENHO_OBSERVACAO = 'EMPENHO_OBSERVACAO'

# 1- CNPJ; 2- CPF
FAVORECIDO_TIPO = 'FAVORECIDO_TIPO'

TIPO_FAVORECIDO_CNPJ = 'CNPJ'
TIPO_FAVORECIDO_CPF = 'CPF'

# Valor Empenhado.
VALOR_EMPENHADO = 'VALOR_EMPENHADO'

# Valor Liquidado
VALOR_LIQUIDADO = 'VALOR_LIQUIDADO'

# Valor Pago.
VALOR_PAGO = 'VALOR_PAGO'

# Restos a Pagar pagos
RP_PAGO = 'RP_PAGO'

"""
TABELA 1B – Informações de empenho por item
"""
####################
## Dados principais
####################

# Número do Empenho (UG + Gestão + Empenho) (Campo chave).
# TODO: O que significa "gestão"? O número do empenho nunca está neste formato.
# TODO: Verificar se nas planilhas já consolidadas que possuem informações de itens de empenho essa informação
# este número está repetido.
DOCUMENTO_NUMERO = 'DOCUMENTO_NUMERO'

# Unidade de medida do item de empenho.
ITEM_EMPENHO_UNIDADE_MEDIDA = 'ITEM_EMPENHO_UNIDADE_MEDIDA'

# Quantidade do item de empenho.
ITEM_EMPENHO_QUANTIDADE = 'ITEM_EMPENHO_QUANTIDADE'

# Valor Unitário do item de empenho.
ITEM_EMPENHO_VALOR_UNITARIO = 'ITEM_EMPENHO_VALOR_UNITARIO'

########################
## Dados secundários
########################

# Descrição do item de empenho.
ITEM_EMPENHO_DESCRICAO = 'ITEM_EMPENHO_DESCRICAO'

# Valor total do item de empenho.
ITEM_EMPENHO_VALOR_TOTAL = 'ITEM_EMPENHO_VALOR_TOTAL'

#################################
## Colunas comumente encontradas
#################################
# Código da categoria econômica (ao que tudo indica, 3 para despesas correntes, 4 para despesas de capital)
CATEGORIA_ECONOMICA_COD = 'CATEGORIA_ECONOMICA_COD'

# Descrição da categoria econômica
CATEGORIA_ECONOMICA_DESCRICAO = 'CATEGORIA_ECONOMICA_DESCRICAO'

CONTA_CORRENTE = 'CONTA_CORRENTE'

DATA_ASSINATURA = 'DATA_ASSINATURA'

DATA_PUBLICACAO = 'DATA_PUBLICACAO'

DATA_CELEBRACAO = 'DATA_CELEBRACAO'

DATA_VIGENCIA = 'DATA_VIGENCIA'

ESPECIE = 'ESPECIE'

FUNDAMENTO_LEGAL = 'FUNDAMENTO_LEGAL'

DATA_INICIO_VIGENCIA = 'DATA_INICIO_VIGENCIA'

DATA_FIM_VIGENCIA = 'DATA_FIM_VIGENCIA'

LINK_CONTRATO = 'LINK_CONTRATO'

LOCAL_EXECUCAO_OU_ENTREGA = 'LOCAL_EXECUCAO_OU_ENTREGA'

NUMERO_CONTRATO = 'NUMERO_CONTRATO'

# Número de processo de compra/aquisição/licitação
NUMERO_PROCESSO = 'NUMERO_PROCESSO'

PRAZO_EM_DIAS = 'PRAZO_EM_DIAS'

SITUACAO = 'SITUACAO'

COLUNAS_DESPESAS = [FONTE_DADOS, DATA_EXTRACAO_DADOS, UF, ESFERA, COD_IBGE_MUNICIPIO, MUNICIPIO_DESCRICAO,
                    CONTRATANTE_CNPJ, CONTRATANTE_DESCRICAO, CONTRATADO_CNPJ, CONTRATADO_DESCRICAO, VALOR_CONTRATO,
                    DESPESA_DESCRICAO, TIPO_DOCUMENTO, DOCUMENTO_NUMERO, DOCUMENTO_DATA]


def consolidar_layout(colunas_adicionais, df_original, dicionario_dados, esfera, fonte_dados, uf, codigo_municipio_ibge,
                      data_extracao, funcao_posprocessamento=None):
    """
    Consolida um conjunto de informações no formato padronizado, convetendo um dataframe de um formato em outro, em
    termos de oolunas.

    :param df_original: O dataframe original.
    :param dicionario_dados: Dicionário que mapeia nomes de colunas no dataframe original nos respectivos nomes de
        coluna no formato padronizado.
    :param colunas_adicionais: Colunas adicionais, que não estão presentes no formato padronizado, mas que
        ainda assim serão incluídas.
    :param uf: Sigla da unidade da federação.
    :param codigo_municipio_ibge: Código do município do IBGE, se aplicável.
    :param fonte_dados: Fonte dos dados (ex.: TCE | TCM | Portal de Transparência - <URL para a fonte dos dados>)
    :param esfera: Esfera administrativa (F- Federal; E- Estadual; e M- Municipal).
    :param data_extracao Data/hora em que os dados foram extraídos.
    :param funcao_posprocessamento: Callback para função que complementa a conversão.
    :return: df: Dataframe resultante.
    """
    df = __converter_dataframes(df_original, dicionario_dados, colunas_adicionais, uf, codigo_municipio_ibge,
                                fonte_dados, esfera, data_extracao)
    if funcao_posprocessamento:
        df = funcao_posprocessamento(df)

    return df


def __converter_dataframes(df_original, dicionario_dados, colunas_adicionais, uf, codigo_municipio_ibge, fonte_dados,
                           esfera, data_extracao):
    df = pd.DataFrame(columns=COLUNAS_DESPESAS)

    for coluna_padronizada, coluna_correspondente in dicionario_dados.items():
        # df[coluna_padronizada] = df_original.get(coluna_correspondente, '')
        df[coluna_padronizada] = df_original.get(coluna_correspondente, np.nan)

    if colunas_adicionais:
        for coluna in colunas_adicionais:
            df[coluna.upper().strip()] = df_original.get(coluna, np.nan)

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
