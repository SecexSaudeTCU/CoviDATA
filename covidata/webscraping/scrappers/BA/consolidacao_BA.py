from datetime import datetime
import logging
from os import path

import pandas as pd

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar



def pre_processar_tce(df):
    # Seleciona apenas os dados da Bahia
    df = df[df['UF'] == '(BA)']

    # Reseta o index
    df.reset_index(drop=True, inplace=True)

    # Junta as colunas "Órgão comprador" e "Text" em uma única
    df['Órgão'] = df[['Órgão comprador', 'Text']].apply(lambda x: ' - '.join(str(x)), axis=1)

    # Eliminhas as colunas especificadas
    df.drop(['UF', 'Órgão comprador', 'Text'], axis=1, inplace=True)

    # Renomeia as colunas especificadas
    df.rename(columns={'Unid.': 'Unidade Item',
                       'Qtde': 'Quantidade Item',
                       'Valor Unit.': 'PU Item',
                       'Valor da compra': 'Preço Item',
                       'Data': 'Data Compra',
                       'Referencia': 'Origem Dados',
                       'Qtd de Compras': 'Quantidade Compras'},
              inplace=True)

    return df


def pos_processar_tce(df):
    for i in range(len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

        if len(str(cpf_cnpj)) >= 14:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        else:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = 'CPF/RG'

    return df


def consolidar_contratos(data_extracao):
    dicionario_dados = {consolidacao.CONTRATADO_DESCRICAO: 'CONTRATADO', consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                        consolidacao.DESPESA_DESCRICAO: 'OBJETO', consolidacao.VALOR_CONTRATO: 'VALOR',
                        consolidacao.LINK_CONTRATO: 'Link para o contrato',
                        consolidacao.NUMERO_CONTRATO: 'Nº DO CONTRATO', consolidacao.PRAZO_EM_DIAS: 'PRAZO',
                        consolidacao.NUMERO_PROCESSO: 'Nº PROCESSO'}
    planilha_original = path.join(config.diretorio_dados, 'BA', 'portal_transparencia', 'contratos.xls')
    df_original = pd.read_excel(planilha_original, header=4)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_BA
    df = consolidar_layout(df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL, fonte_dados, 'BA', '',
                           data_extracao, None)
    df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
    return df


def consolidar_tce(data_extracao):
    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.DESPESA_DESCRICAO: 'Item de Exibição',
                        consolidacao.CONTRATANTE_DESCRICAO: 'Órgão',
                        consolidacao.CONTRATADO_DESCRICAO: 'Credor',
                        consolidacao.CONTRATADO_CNPJ: 'CNPJ Fornecedor'}

    # Lê o arquivo "xlsx" de aquisições nacionais baixado como um objeto pandas DataFrame
    df_original = pd.read_excel(path.join(config.diretorio_dados, 'BA', 'tce',
                                          'MiranteRelDadosAbertosPainelPublicoCovid19Nacional.xlsx'),
                                usecols=[0, 1] + list(range(3, 16)))

    # Chama a função "pre_processar_tce" definida neste módulo
    df = pre_processar_tce(df_original)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_BA, 'BA', '',
                           data_extracao, pos_processar_tce)

    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Bahia')

    consolidacoes = consolidar_contratos(data_extracao)
    consolidacao_tce_BA = consolidar_tce(data_extracao)

    consolidacoes = consolidacoes.append(consolidacao_tce_BA, ignore_index=True, sort=False)

    salvar(consolidacoes, 'BA')

#consolidar(datetime.now())