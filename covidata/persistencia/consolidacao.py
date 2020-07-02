import pandas as pd
from covidata import config

ANO_PADRAO = 2020

"""
TABELA 1 – Informações relacionadas às despesas nos estados e municípios
"""
# Descrição da fonte de extração dos dados.
FONTE_DADOS = 'FONTE_DADOS'

TIPO_FONTE_TCE = 'TCE'
TIPO_FONTE_PORTAL_TRANSPARENCIA = 'Portal de Transparência'

# Registro da data de extração dos dados.
DATA_EXTRACAO_DADOS = 'DATA_EXTRACAO_DADOS'

# Ano da execução da despesa.
ANO = 'ANO'

# UF da Unidade Gestora.
UF = 'UF'

# F- Federal; E- Estadual; e M- Municipal.
ESFERA = 'ESFERA'

ESFERA_FEDERAL = 'F'
ESFERA_ESTADUAL = 'E'
ESFERA_MUNICIPAL = 'M'

# Código do Município (IBGE).
COD_MUNICIPIO_IBGE = 'COD_MUNICIPIO_IBGE'

# Código da Fonte dos Recursos.
FONTE_RECURSOS_COD = 'FONTE_RECURSOS_COD'

# Descrição da Fonte dos Recursos.
FONTE_RECURSOS_DESCRICAO = 'FONTE_RECURSOS_DESCRICAO'

# Código numérico do Órgão.
ORGAO_COD = 'ORGAO_COD'

# Descrição do nome do Órgão.
ORGAO_DESCRICAO = 'ORGAO_DESCRICAO'

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

# Número do Empenho (UG + Gestão + Empenho) (Campo chave).
EMPENHO_NUMERO = 'EMPENHO_NUMERO'

# Descrição da observação do empenho.
EMPENHO_OBSERVACAO = 'EMPENHO_OBSERVACAO'

# Data de emissão do empenho.
EMPENHO_DATA = 'EMPENHO_DATA'

# Código do favorecido, CNPJ ou CPF. (Campo chave).
FAVORECIDO_COD = 'FAVORECIDO_COD'

# Descrição do favorecido do empenho.
FAVORECIDO_DESCRICAO = 'FAVORECIDO_DESCRICAO'

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
# Número do Empenho (UG + Gestão + Empenho) (Campo chave).
EMPENHO_NUMERO = 'EMPENHO_NUMERO'

# Descrição do item de empenho.
ITEM_EMPENHO_DESCRICAO = 'ITEM_EMPENHO_DESCRICAO'

# Unidade de medida do item de empenho.
ITEM_EMPENHO_UNIDADE_MEDIDA = 'ITEM_EMPENHO_UNIDADE_MEDIDA'

# Quantidade do item de empenho.
ITEM_EMPENHO_QUANTIDADE = 'ITEM_EMPENHO_QUANTIDADE'

# Valor Unitário do item de empenho.
ITEM_EMPENHO_VALOR_UNITARIO = 'ITEM_EMPENHO_VALOR_UNITARIO'

# Valor total do item de empenho.
ITEM_EMPENHO_VALOR_TOTAL = 'ITEM_EMPENHO_VALOR_TOTAL'

COLUNAS_DESPESAS = [FONTE_DADOS, DATA_EXTRACAO_DADOS, ANO, UF, ESFERA, COD_MUNICIPIO_IBGE, FONTE_RECURSOS_COD,
                    FONTE_RECURSOS_DESCRICAO, ORGAO_COD, ORGAO_DESCRICAO, UG_COD, UG_CNPJ, UG_DESCRICAO, FUNCIONAL,
                    FUNCAO_COD, FUNCAO_DESCRICAO, SUBFUNCAO_COD, SUBFUNCAO_DESCRICAO, PROGRAMA_COD, PROGRAMA_DESCRICAO,
                    ACAO_COD, ACAO_DESCRICAO, SUBTITULO_COD, SUBTITULO_DESCRICAO, MOD_APLICACAO_COD,
                    MOD_APLIC_DESCRICAO, GND_COD, GND_DESCRICAO, ELEMENTO_DESPESA_COD, ELEMENTO_DESPESA_DESCRICAO,
                    SUB_ELEMENTO_DESPESA_COD, SUB_ELEMENTO_DESPESA_DESCRICAO, EMPENHO_NUMERO, EMPENHO_OBSERVACAO,
                    EMPENHO_DATA, FAVORECIDO_COD, FAVORECIDO_DESCRICAO, FAVORECIDO_TIPO, VALOR_EMPENHADO,
                    VALOR_LIQUIDADO, VALOR_PAGO, RP_PAGO]

COLUNAS_ITENS_EMPENHO = [EMPENHO_NUMERO, ITEM_EMPENHO_DESCRICAO, ITEM_EMPENHO_UNIDADE_MEDIDA, ITEM_EMPENHO_QUANTIDADE,
                         ITEM_EMPENHO_VALOR_UNITARIO, ITEM_EMPENHO_VALOR_TOTAL]


def consolidar(ano, colunas_adicionais_despesas, df_original, dicionario_dados, esfera, fonte_dados, uf,
               codigo_municipio_ibge, funcao_posprocessamento):
    """
    Consolida um conjunto de informações no formato padronizado, convetendo um dataframe de um formato em outro, em
    termos de oolunas.

    :param ano: Ano da execução da despesa.
    :param colunas_adicionais_despesas: Colunas adicionais, que não estão presentes no formato padronizado, mas que
        ainda assim serão incluídas.
    :param df_original: O dataframe original.
    :param dicionario_dados: Dicionário que mapeia nomes de colunas no dataframe original nos respectivos nomes de
        coluna no formato padronizado.
    :param esfera: Esfera administrativa (F- Federal; E- Estadual; e M- Municipal).
    :param fonte_dados: Fonte dos dados (ex.: TCE | TCM | Portal de Transparência - <URL para a fonte dos dados>)
    :param uf: Sigla da unidade da federação.
    :param codigo_municipio_ibge: Código do município do IBGE, se aplicável.
    :param funcao_posprocessamento: Callback para função que complementa a conversão.
    :return: df_despesas, df_itens_empenho: Tupla que representa os dois dataframes (tabelas): despesas e itens de
        empenho.
    """
    df_despesas, df_itens_empenho = __converter_dataframes(df_original, dicionario_dados, colunas_adicionais_despesas,
                                                           [], uf,
                                                           codigo_municipio_ibge, fonte_dados, ano, esfera)
    return funcao_posprocessamento(df_original, df_despesas, df_itens_empenho)


def __converter_dataframes(df_original, dicionario_dados, colunas_adicionais_despesas, colunas_adicionais_itens_empenho,
                           uf, codigo_municipio_ibge, fonte_dados, ano, esfera):
    df_despesas = pd.DataFrame(columns=COLUNAS_DESPESAS + colunas_adicionais_despesas)
    df_itens_empenho = pd.DataFrame(columns=COLUNAS_ITENS_EMPENHO + colunas_adicionais_itens_empenho)

    for coluna_padronizada, coluna_correspondente in dicionario_dados.items():
        if coluna_padronizada in COLUNAS_DESPESAS:
            df_despesas[coluna_padronizada] = df_original[coluna_correspondente]
        else:
            df_itens_empenho[coluna_padronizada] = df_original[coluna_correspondente]

    for coluna in colunas_adicionais_despesas:
        df_despesas[coluna] = df_original[coluna]

    for coluna in colunas_adicionais_itens_empenho:
        df_itens_empenho[coluna] = df_original[coluna]

    df_despesas[FONTE_DADOS] = fonte_dados

    df_despesas[DATA_EXTRACAO_DADOS] = __get_data_extracao()
    df_despesas[ANO] = ano
    df_despesas[UF] = uf
    df_despesas[COD_MUNICIPIO_IBGE] = codigo_municipio_ibge
    df_despesas[ESFERA] = esfera

    return df_despesas, df_itens_empenho


def __get_data_extracao():
    data_extracao_dados = ''
    f = open(config.arquivo_data_extracao, "r")

    if f.mode == 'r':
        data_extracao_dados = f.read()

    return data_extracao_dados
