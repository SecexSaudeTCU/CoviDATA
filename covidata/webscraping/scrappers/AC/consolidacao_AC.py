import logging
from os import path

import numpy as np
import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_municipios_por_uf, get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


# TODO: Para o portal de transparência, o arquivo CSV disponibilizado não tem os nomes das colunas, não sendo possível
#  portanto entende-lo ao ponto de consolidar as informações.

def pos_processar_despesas(df):
    # Elimina a última linha, que só contém um totalizador
    df = df.drop(df.index[-1])

    df = df.astype({consolidacao.CONTRATADO_CNPJ: np.uint64, consolidacao.DOCUMENTO_NUMERO: np.uint64})
    df = df.astype({consolidacao.CONTRATADO_CNPJ: str, consolidacao.DOCUMENTO_NUMERO: str})

    df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'
    df.fillna('')

    for i in range(0, len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

        if len(cpf_cnpj) == 11:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
        elif len(cpf_cnpj) > 11:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    return df


def pos_processar_despesas_municipio(df):
    # Elimina a última linha, que só contém um totalizador
    df = df.drop(df.index[-1])
    df.fillna('')

    for i in range(0, len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ].strip()

        if len(cpf_cnpj) == 14:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
        elif len(cpf_cnpj) > 14:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    df = __definir_municipios(df, prefixo='\nPrefeitura Municipal de ')

    return df


def pos_processar_contratos_municipios(df):
    # Elimina as três últimas linhas
    df.drop(df.tail(2).index, inplace=True)
    df = __definir_municipios(df)
    return df


def __definir_municipios(df, prefixo='PREFEITURA MUNICIPAL DE\r\n '):
    codigos_municipios = get_municipios_por_uf('AC')
    # Define os municípios
    df[consolidacao.MUNICIPIO_DESCRICAO] = df.apply(
        lambda row: __get_nome_municipio(row, prefixo), axis=1)
    df[consolidacao.COD_IBGE_MUNICIPIO] = df.apply(
        lambda row: codigos_municipios.get(row[consolidacao.MUNICIPIO_DESCRICAO].upper(), ''), axis=1)
    df = df.astype({consolidacao.COD_IBGE_MUNICIPIO: str})
    return df


def pos_processar_dispensas(df):
    # Elimina a última linha, que só contém um totalizador
    df = df.drop(df.index[-1])
    df[consolidacao.MOD_APLIC_DESCRICAO] = 'Dispensa de Licitação'
    return df


def pos_processar_dispensas_municipios(df):
    df = pos_processar_dispensas(df)
    df = __definir_municipios(df, prefixo='\nPREFEITURA MUNICIPAL DE ')
    df = df.rename(columns={'DATA\r\n  DA ALIMENTAÇÃO': 'DATA DA ALIMENTAÇÃO'})
    df[consolidacao.MOD_APLIC_DESCRICAO] = 'Dispensa de Licitação'
    return df


def pos_processar_portal_transparencia_estadual(df):
    for i in range(len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

        if len(str(cpf_cnpj)) >= 14:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        else:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = 'CPF/RG'

    return df


def pos_processar_portal_transparencia_capital(df):
    # Elimina as sete últimas linhas
    df.drop(df.tail(7).index, inplace=True)
    df = df.astype({consolidacao.ANO: np.uint16, consolidacao.NUMERO_CONTRATO: np.int64})
    df[consolidacao.MUNICIPIO_DESCRICAO] = 'RIO BRANCO'
    return df


def __get_nome_municipio(row, prefixo='\nPrefeitura Municipal de '):
    string_original = row[consolidacao.CONTRATANTE_DESCRICAO]

    if prefixo in string_original:
        nome_municipio = string_original[len(prefixo):len(string_original)].strip()
        return nome_municipio
    else:
        return ''


def __consolidar_despesas(data_extracao):
    dicionario_dados = {consolidacao.DOCUMENTO_NUMERO: '\nNUMEROEMPENHO\n',
                        consolidacao.CONTRATADO_DESCRICAO: '\nRazão Social\n',
                        consolidacao.CONTRATADO_CNPJ: '\nCPF/CNPJ\n',
                        consolidacao.DOCUMENTO_DATA: '\nData do Empenho\n',
                        consolidacao.FONTE_RECURSOS_COD: '\nFonte de Recurso\n',
                        consolidacao.VALOR_EMPENHADO: '\nValor Empenhado\r\n  ($)\n',
                        # TODO: Dúvida: Podemos assumir para VALOR_R$ o valor empenhado, quando essa informação não estiver disponível?
                        consolidacao.VALOR_CONTRATO: '\nValor Empenhado\r\n  ($)\n'}
    # TODO: Sugerir uma nova coluna 'TIPO_FONTE'
    # TODO: Nem sempre esta informação está presente
    df_original = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'despesas.xls'), header=4)
    df = consolidar_layout(['\nTipo de Credor\n'], df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_despesas, 'AC', '', data_extracao,
                           pos_processar_despesas)
    return df


def __consolidar_despesas_municipios(data_extracao):
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: '\nPREFEITURAS MUNICIPAIS NO ESTADO DO ACRE\n',
                        consolidacao.UG_DESCRICAO: '\nPREFEITURAS MUNICIPAIS NO ESTADO DO ACRE\n',
                        consolidacao.CONTRATADO_CNPJ: '\nCNPJ/CPF\n',
                        consolidacao.VALOR_CONTRATO: '\n VALOR CONTRATADO R$\n'}
    colunas_adicionais = ['\nCONTRATOS/OBSERVAÇÕES\n']

    df_original = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'despesas_municipios.xls'), header=4)
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_despesas_municipios, 'AC', '',
                           data_extracao, pos_processar_despesas_municipio)
    return df


def __consolidar_contratos(data_extracao):
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Entidade', consolidacao.DESPESA_DESCRICAO: ' Objeto ',
                        consolidacao.VALOR_CONTRATO: 'Valor R$', consolidacao.UG_DESCRICAO: 'Entidade',
                        consolidacao.FUNDAMENTO_LEGAL: '\xa0Fundamento Legal\xa0',
                        consolidacao.NUMERO_PROCESSO: 'Nº Processo'}
    colunas_adicionais = ['Cód.\r\n  Dispensa', 'Data Pedido']

    df_original = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'contratos.xls'), header=4)
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_contratos, 'AC', '', data_extracao)
    # Elimina as duas últimas linhas
    df.drop(df.tail(2).index, inplace=True)
    return df


def __consolidar_contratos_municipios(data_extracao):
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Entidade', consolidacao.DESPESA_DESCRICAO: ' Objeto ',
                        consolidacao.VALOR_CONTRATO: 'Valor R$', consolidacao.UG_DESCRICAO: 'Entidade',
                        consolidacao.FUNDAMENTO_LEGAL: '\xa0Fundamento Legal\xa0',
                        consolidacao.NUMERO_PROCESSO: 'Nº Processo'}
    colunas_adicionais = ['Cód.\r\n  Dispensa', 'Data Pedido']

    df_original = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'contratos_municipios.xls'), header=4)
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_contratos_municipios, 'AC', '',
                           data_extracao, pos_processar_contratos_municipios)
    return df


def __consolidar_dispensas(data_extracao):
    dicionario_dados = {consolidacao.DESPESA_DESCRICAO: '\nObjeto\n', consolidacao.VALOR_CONTRATO: '\nValor\r\n  R$\n',
                        consolidacao.CONTRATANTE_DESCRICAO: '\nEnte\n', consolidacao.UG_DESCRICAO: '\nEnte\n',
                        consolidacao.CONTRATADO_DESCRICAO: '\nFornecedor\n',
                        consolidacao.NUMERO_PROCESSO: '\nNúmero\r\n  Processo\n'}
    colunas_adicionais = ['\nData da Alimentação\n']
    df_original = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'dispensas.xls'), header=4)
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_despesas, 'AC', '', data_extracao,
                           pos_processar_dispensas)
    return df


def __consolidar_dispensas_municipios(data_extracao):
    dicionario_dados = {consolidacao.DESPESA_DESCRICAO: '\nObjeto\n', consolidacao.VALOR_CONTRATO: '\nValor\r\n  R$\n',
                        consolidacao.CONTRATANTE_DESCRICAO: '\nEnte\n', consolidacao.UG_DESCRICAO: '\nEnte\n',
                        consolidacao.CONTRATADO_DESCRICAO: '\nFornecedor\n',
                        consolidacao.NUMERO_PROCESSO: '\nNúmero\r\n  Processo\n'}
    colunas_adicionais = ['\nData\r\n  da Alimentação\n']
    df_original = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'dispensas_municipios.xls'), header=4)
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_despesas_municipios, 'AC', '',
                           data_extracao, pos_processar_dispensas_municipios)
    return df


def __consolidar_portal_transparencia_estadual(data_extracao):

    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão',
                        consolidacao.VALOR_EMPENHADO: 'Valor Empenhado',
                        consolidacao.NUMERO_CONTRATO: 'Número Contrato',
                        consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                        consolidacao.CONTRATADO_DESCRICAO: 'Nome Contratada',
                        consolidacao.DATA_INICIO_VIGENCIA: 'Data Início Vigência',
                        consolidacao.DATA_FIM_VIGENCIA: 'Data Fim Vigência',
                        consolidacao.DOCUMENTO_NUMERO: 'Número Empenho',
                        consolidacao.DOCUMENTO_DATA: 'Data Empenho',
                        consolidacao.ACAO_COD: 'Código Ação',
                        consolidacao.ACAO_DESCRICAO: 'Descrição Ação'}

    # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
    colunas_adicionais = ['Situação Contratual']

    # Lê o arquivo "csv" de empenhos baixado como um objeto pandas DataFrame
    df_original = pd.read_csv(
        path.join(config.diretorio_dados, 'AC', 'portal_transparencia', 'empenhos.csv'))

    # Remove notação científica das colunas especificadas do objeto pandas DataFrame "df_original"
    df_original['11'] = df_original['11'].apply(lambda x: '%.0f' % x)

    # Seleciona as colunas de "df_original" a integrar "df"
    df = df_original[['1', '2', '6', '7', '8', '9', '10', '11', '12', '16', '17']]

    # Renomeia as colunas de "df" para nomes inteligíveis
    df.rename(columns={'1': 'Órgão',
                       '2': 'Situação Contratual',
                       '5': 'Valor Empenhado',
                       '6': 'Número Contrato',
                       '7': 'CNPJ',
                       '8': 'Nome Contratada',
                       '9': 'Data Início Vigência',
                       '10': 'Data Fim Vigência',
                       '11': 'Número Empenho',
                       '12': 'Data Empenho',
                       '16': 'Código Ação',
                       '17': 'Descrição Ação'},
              inplace=True)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(colunas_adicionais, df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_AC, 'AC', '',
                           data_extracao, pos_processar_portal_transparencia_estadual)

    return df


def __consolidar_portal_transparencia_capital(data_extracao):
    dicionario_dados = {consolidacao.ANO: 'Exercício', consolidacao.CONTRATADO_DESCRICAO: 'Fornecedor',
                        consolidacao.DESPESA_DESCRICAO: 'Objeto', consolidacao.MOD_APLIC_DESCRICAO: 'Modalidade',
                        consolidacao.CONTRATANTE_DESCRICAO: 'Secretaria', consolidacao.VALOR_CONTRATO: 'Valor',
                        consolidacao.UG_DESCRICAO: 'Secretaria', consolidacao.DATA_ASSINATURA: 'Data de Assinatura',
                        consolidacao.NUMERO_CONTRATO: 'Número do Contrato',
                        consolidacao.NUMERO_PROCESSO: 'Número do Processo',
                        consolidacao.DATA_FIM_VIGENCIA: 'Prazo de Vigência'}
    df_original = pd.read_excel(
        path.join(config.diretorio_dados, 'AC', 'portal_transparencia', 'Rio Branco', 'webexcel.xls'), header=11)
    df = consolidar_layout([], df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_RioBranco, 'AC',
                           get_codigo_municipio_por_nome('Rio Branco', 'AC'), data_extracao,
                           pos_processar_portal_transparencia_capital)
    return df


def consolidar(data_extracao):
    # TODO: Pode ser recomendável unificar os formatos de CPF e CNPJ para remover pontos e hífens.
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Acre')
    despesas = __consolidar_despesas(data_extracao)

    despesas_municipios = __consolidar_despesas_municipios(data_extracao)
    despesas = despesas.append(despesas_municipios, ignore_index=True, sort=False)

    despesas.fillna('')

    contratos = __consolidar_contratos(data_extracao)
    despesas = despesas.append(contratos, ignore_index=True, sort=False)

    contratos_municipios = __consolidar_contratos_municipios(data_extracao)
    despesas = despesas.append(contratos_municipios, ignore_index=True, sort=False)

    dispensas = __consolidar_dispensas(data_extracao)
    despesas = despesas.append(dispensas, ignore_index=True, sort=False)

    dispensas_municipios = __consolidar_dispensas_municipios(data_extracao)
    despesas = despesas.append(dispensas_municipios, ignore_index=True, sort=False)

    #TODO: Site indisponível
    #pt_estadual = __consolidar_portal_transparencia_estadual(data_extracao)
    #despesas = despesas.append(pt_estadual, ignore_index=True, sort=False)

    pt_capital = __consolidar_portal_transparencia_capital(data_extracao)
    despesas = despesas.append(pt_capital, ignore_index=True, sort=False)

    salvar(despesas, 'AC')
