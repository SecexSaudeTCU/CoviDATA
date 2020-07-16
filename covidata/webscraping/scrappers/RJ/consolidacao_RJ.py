import logging
import os
from os import path

import numpy as np
import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pos_processar_despesas_capital(df):
    df[consolidacao.MUNICIPIO_DESCRICAO] = 'Rio de Janeiro'

    # Remove notação científica
    df = df.astype({consolidacao.CONTRATADO_CNPJ: np.uint64, consolidacao.NUMERO_PROCESSO: np.int64})
    df = df.astype({consolidacao.CONTRATADO_CNPJ: str, consolidacao.NUMERO_PROCESSO: str})

    __processar_tipo_favorecido(df)

    return df


def pos_processar_contratos_capital(df):
    df[consolidacao.MUNICIPIO_DESCRICAO] = 'Rio de Janeiro'
    df = df.astype({consolidacao.CONTRATADO_CNPJ: str})

    __processar_tipo_favorecido(df)

    df = df.rename(
        columns={'UNIDADE ORÇAMENTÁRIA EXECUTORA': 'UO', 'DESCRIÇÃO DA UNIDADE ORÇAMENTÁRIA EXECUTORA': 'NOMEUO',
                 'ÓRGÃO EXECUTOR': 'ORGAO'})
    return df


def pos_processar_favorecidos_capital(df):
    df[consolidacao.MUNICIPIO_DESCRICAO] = 'Rio de Janeiro'

    # Remove notação científica
    df = df.astype({consolidacao.CONTRATADO_CNPJ: np.uint64})
    df = df.astype({consolidacao.CONTRATADO_CNPJ: str})

    df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'
    df = df.rename(columns={'UNIDADE ORÇAMENTÁRIA EXECUTORA': 'UO',
                            'DESCRIÇÃO DA UNIDADE ORÇAMENTÁRIA EXECUTORA': 'NOMEUO', 'ÓRGÃO EXECUTOR': 'ORGAO',
                            'MODALIDADE': 'MODALIDADE DE LICITAÇÃO', 'AGENCIA BANCO': 'AGENCIA',
                            'DESCRIÇÃO DA NATUREZA': 'DESCRIÇÃO DA NATUREZA DA DESPESA'})
    return df


def __processar_tipo_favorecido(df):
    for i in range(0, len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

        if len(cpf_cnpj) >= 11:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ


def __consolidar_despesas_capital(data_extracao):
    dicionario_dados = {consolidacao.GND_DESCRICAO: 'Grupo', consolidacao.MOD_APLIC_DESCRICAO: 'Modalidade',
                        consolidacao.ELEMENTO_DESPESA_COD: 'Elemento',
                        consolidacao.ELEMENTO_DESPESA_DESCRICAO: 'NomeElemento',
                        consolidacao.SUB_ELEMENTO_DESPESA_COD: 'SubElemento',
                        consolidacao.SUB_ELEMENTO_DESPESA_DESCRICAO: 'NomeSubElemento',
                        consolidacao.UG_COD: 'UG', consolidacao.UG_DESCRICAO: 'NomeUG',
                        consolidacao.CONTRATANTE_DESCRICAO: 'NomeOrgao', consolidacao.CONTRATADO_CNPJ: 'Credor',
                        consolidacao.CONTRATADO_DESCRICAO: 'NomeCredor',
                        consolidacao.FONTE_RECURSOS_COD: 'FonteRecursos',
                        consolidacao.FONTE_RECURSOS_DESCRICAO: 'NomeFonteRecursos', consolidacao.FUNCAO_COD: 'Funcao',
                        consolidacao.FUNCAO_DESCRICAO: 'NomeFuncao', consolidacao.SUBFUNCAO_COD: 'SubFuncao',
                        consolidacao.SUBFUNCAO_DESCRICAO: 'NomeSubFuncao', consolidacao.PROGRAMA_COD: 'Programa',
                        consolidacao.PROGRAMA_DESCRICAO: 'NomePrograma', consolidacao.ACAO_COD: 'Acao',
                        consolidacao.ACAO_DESCRICAO: 'NomeAcao', consolidacao.VALOR_CONTRATO: 'Valor',
                        consolidacao.ANO: 'Exercicio', consolidacao.DOCUMENTO_NUMERO: 'EmpenhoExercicio',
                        consolidacao.DOCUMENTO_DATA: 'Data', consolidacao.TIPO_DOCUMENTO: 'TipoAto',
                        consolidacao.DESPESA_DESCRICAO: 'ObjetoContrato', consolidacao.CONTA_CORRENTE: 'ContaCorrente',
                        consolidacao.FUNDAMENTO_LEGAL: 'Legislacao', consolidacao.NUMERO_CONTRATO: 'NumeroContrato',
                        consolidacao.NUMERO_PROCESSO: 'processo'}
    colunas_adicionais = ['Poder', 'UO', 'NomeUO', 'Orgao', 'Licitacao', 'Liquidacao', 'Pagamento', 'Banco',
                          'NomeBanco', 'Agencia', 'NomeContaCorrente', 'ASPS', 'MDE', 'ExercicioContrato', 'Historico']
    planilha_original = path.join(config.diretorio_dados, 'RJ', 'portal_transparencia', 'Rio de Janeiro',
                                  '_arquivos_Open_Data_Desp_Ato_Covid19_2020.txt')
    df_original = pd.read_csv(planilha_original, sep=';', encoding='ISO-8859-1')
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Rio_despesas_por_ato
    codigo_municipio_ibge = get_codigo_municipio_por_nome('Rio de Janeiro', 'RJ')
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           fonte_dados, 'RJ', codigo_municipio_ibge, data_extracao, pos_processar_despesas_capital)
    return df


def __consolidar_contratos_capital(data_extracao):
    dicionario_dados = {consolidacao.ANO: 'Ano instrumento', consolidacao.CONTRATADO_CNPJ: 'Código favorecido',
                        consolidacao.CONTRATADO_DESCRICAO: 'Favorecido',
                        consolidacao.FONTE_RECURSOS_COD: 'Fonte de recursos',
                        consolidacao.FONTE_RECURSOS_DESCRICAO: 'Descrição da fonte de recursos',
                        consolidacao.DESPESA_DESCRICAO: 'Objeto',
                        consolidacao.VALOR_EMPENHADO: 'Valor empenhado',
                        consolidacao.VALOR_LIQUIDADO: 'Valor liquidado', consolidacao.VALOR_PAGO: 'Valor pago',
                        consolidacao.CONTRATANTE_DESCRICAO: 'Descrição do órgão executor',
                        consolidacao.UG_DESCRICAO: 'Descrição do órgão executor',
                        consolidacao.PROGRAMA_COD: 'Programa', consolidacao.PROGRAMA_DESCRICAO: 'Descrição de programa',
                        consolidacao.GND_COD: 'Grupo de despesa',
                        consolidacao.GND_DESCRICAO: 'Descrição de grupo de despesa ',
                        consolidacao.ELEMENTO_DESPESA_COD: 'Elemento de despesa',
                        consolidacao.ELEMENTO_DESPESA_DESCRICAO: 'Descrição do elemento de despesa',
                        # TODO: Em todos os demais consolidadores, substituir o significado destas colunas de modalidade.
                        consolidacao.MOD_APLICACAO_COD: 'Modalidade de aplicação',
                        consolidacao.MOD_APLIC_DESCRICAO: 'Descrição da modalidade de aplicação',
                        consolidacao.VALOR_CONTRATO: 'Valor atualizado do instrumento',
                        consolidacao.CATEGORIA_ECONOMICA_COD: 'Categoria Econômica',
                        consolidacao.CATEGORIA_ECONOMICA_DESCRICAO: 'Descrição da categoria econômica',
                        consolidacao.ESPECIE: 'Espécie', consolidacao.DATA_FIM_PREVISTO: 'Data fim previsto',
                        consolidacao.FUNDAMENTO_LEGAL: 'Fundamentação Legal',
                        consolidacao.NUMERO_PROCESSO: 'Processo instrutivo', consolidacao.SITUACAO: 'Situação'
                        }
    colunas_adicionais = ['Nr instrumento', 'Data início previsto', 'Valor inicial do instrumento',
                          'Valor do acréscimo ou redução', 'Valor atualizado do instrumento',
                          'Saldo a executar do instrumento', 'Data da assinatura', 'Data do encerramento',
                          'Órgão executor', 'Unidade orçamentária executora',
                          'Descrição da unidade orçamentária executora', 'Modalidade de licitação',
                          'Natureza da despesa', 'Descrição da natureza da despesa', 'Poder', 'Tipo Administração',
                          'Dir Ind', 'Programa de trabalho']
    planilha_original = path.join(config.diretorio_dados, 'RJ', 'portal_transparencia', 'Rio de Janeiro',
                                  'Open_Data_Contratos_Covid19_2020.xlsx')
    df_original = pd.read_excel(planilha_original)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Rio_contratos
    codigo_municipio_ibge = get_codigo_municipio_por_nome('Rio de Janeiro', 'RJ')
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           fonte_dados, 'RJ', codigo_municipio_ibge, data_extracao, pos_processar_contratos_capital)
    return df


def __consolidar_favorecidos_capital(data_extracao):
    dicionario_dados = {consolidacao.ANO: 'Exercício do empenho', consolidacao.DOCUMENTO_NUMERO: 'Número do empenho',
                        consolidacao.VALOR_LIQUIDADO: 'Vl liq', consolidacao.VALOR_PAGO: 'Vl pago',
                        consolidacao.CONTRATADO_DESCRICAO: 'Favorecido',
                        consolidacao.CONTRATADO_CNPJ: 'Código favorecido',
                        consolidacao.FAVORECIDO_TIPO: 'Descrição do tipo de favorecido',
                        consolidacao.PROGRAMA_COD: 'Programa',
                        consolidacao.PROGRAMA_DESCRICAO: 'Descrição de programa',
                        consolidacao.FONTE_RECURSOS_COD: 'Fonte de recursos',
                        consolidacao.FONTE_RECURSOS_DESCRICAO: 'Descrição da fonte de recursos',
                        consolidacao.CONTRATANTE_DESCRICAO: 'Descrição do órgão executor',
                        consolidacao.UG_DESCRICAO: 'Descrição do órgão executor',
                        consolidacao.GND_COD: 'Grupo de despesa',
                        consolidacao.GND_DESCRICAO: 'Descrição de grupo de despesa ',
                        consolidacao.ELEMENTO_DESPESA_COD: 'Elemento de despesa',
                        consolidacao.ELEMENTO_DESPESA_DESCRICAO: 'Descrição do elemento de despesa',
                        consolidacao.MOD_APLICACAO_COD: 'Modalidade de aplicação',
                        consolidacao.MOD_APLIC_DESCRICAO: 'Descrição da modalidade de aplicação',
                        consolidacao.CATEGORIA_ECONOMICA_COD: 'Categoria Econômica',
                        consolidacao.CATEGORIA_ECONOMICA_DESCRICAO: 'Descrição da categoria econômica',
                        consolidacao.CONTA_CORRENTE: 'Conta banco',
                        consolidacao.FUNDAMENTO_LEGAL: 'Fundamentação Legal'}
    colunas_adicionais = ['Órgão do empenho', 'Descrição do órgão do empenho', 'Órgão programa de trabalho',
                          'Unidade programa de trabalho', 'Natureza da despesa', 'Descrição da natureza',
                          'Data da liquidação', 'Data do pagamento', 'Número da liquidação', 'Vl anul liq',
                          'vl anul liq rp', 'Dt anul liq', 'Dt anul pag', 'Vl anul disp', 'Vl anul emp', 'Vl anul rp',
                          'Processo de liquidação', 'Processo do empenho', 'Vl Soma Retencao', 'Vl inss', 'Vl iss',
                          'Vl ir', 'Vl descontos', 'Vl multas', 'Vl csll', 'Vl cofins', 'Vl pis pasep',
                          'Vl cofins pis pasep csll', 'Vl tafi', 'Vl tafc', 'Vl trfc', 'Modalidade', 'Banco',
                          'Agencia banco', 'Órgão instrumento', 'Nr instrumento', 'Órgão executor',
                          'Unidade orçamentária executora', 'Descrição da unidade orçamentária executora',
                          'Programa de trabalho', 'Descrição do programa de trabalho', 'Vl sem retencao',
                          'Número licitacao', 'Poder', 'Tipo Administração', 'Dir Ind']
    planilha_original = path.join(config.diretorio_dados, 'RJ', 'portal_transparencia', 'Rio de Janeiro',
                                  'Open_Data_Favorecidos_Covid19_2020.xlsx')
    df_original = pd.read_excel(planilha_original)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Rio_favorecidos
    codigo_municipio_ibge = get_codigo_municipio_por_nome('Rio de Janeiro', 'RJ')
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           fonte_dados, 'RJ', codigo_municipio_ibge, data_extracao, pos_processar_favorecidos_capital)
    return df


def __consolidar_compras_diretas(data_extracao):
    dicionario_dados = {consolidacao.DESPESA_DESCRICAO: 'Objeto', consolidacao.CONTRATANTE_DESCRICAO: 'Unidade',
                        consolidacao.UG_DESCRICAO: 'Unidade', consolidacao.CONTRATADO_DESCRICAO: 'Fornecedor',
                        consolidacao.ITEM_EMPENHO_QUANTIDADE: 'Qtd', consolidacao.VALOR_CONTRATO: 'Valor Processo',
                        consolidacao.ITEM_EMPENHO_UNIDADE_MEDIDA: 'Unidade Medida',
                        consolidacao.ITEM_EMPENHO_DESCRICAO: 'Item',
                        consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'Valor Unitário',
                        consolidacao.FUNDAMENTO_LEGAL: 'Enquadramento Legal', consolidacao.NUMERO_PROCESSO: 'Processo'}
    colunas_adicionais = ['Afastamento', 'Data Aprovação']

    # Renomeia o arquivo
    diretorio = path.join(config.diretorio_dados, 'RJ')
    arquivos = os.listdir(diretorio)

    nome_arquivo = 'tce_rj_compras_diretas.xlsx'
    for arquivo in arquivos:
        if '.xlsx' in arquivo:
            os.rename(path.join(diretorio, arquivo), path.join(diretorio, nome_arquivo))
            break

    planilha_original = path.join(config.diretorio_dados, 'RJ', nome_arquivo)
    df_original = pd.read_excel(planilha_original)
    fonte_dados = consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_RJ
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'RJ', '', data_extracao)
    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Rio de Janeiro')

    df = __consolidar_compras_diretas(data_extracao)

    despesas_capital = __consolidar_despesas_capital(data_extracao)
    df = df.append(despesas_capital)

    contratos_capital = __consolidar_contratos_capital(data_extracao)
    df = df.append(contratos_capital)

    favorecidos_capital = __consolidar_favorecidos_capital(data_extracao)
    df = df.append(favorecidos_capital)

    salvar(df, 'RJ')
