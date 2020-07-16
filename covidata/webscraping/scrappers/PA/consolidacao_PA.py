import logging
import os
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pos_processar_portal_transparencia_estadual(df):
    for i in range(0, len(df)):
        cpf_cnpj = str(df.loc[i, consolidacao.CONTRATADO_CNPJ])

        if len(cpf_cnpj) == 14 and '.' in cpf_cnpj and '-' in cpf_cnpj:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
        elif len(cpf_cnpj) > 14 or (len(cpf_cnpj) == 14 and not '.' in cpf_cnpj and not '-' in cpf_cnpj):
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    return df


def pos_processar_portal_transparencia_capital(df):
    df['temp'] = df[consolidacao.UG_DESCRICAO]
    df[consolidacao.UG_COD] = df.apply(lambda row: row['temp'][0:row['temp'].find('-')], axis=1)
    df[consolidacao.UG_DESCRICAO] = df.apply(lambda row: row['temp'][row['temp'].find('-') + 1:len(row['temp'])],
                                             axis=1)
    df = df.drop(['temp'], axis=1)

    df['temp'] = df[consolidacao.FONTE_RECURSOS_DESCRICAO]
    df[consolidacao.FONTE_RECURSOS_COD] = df.apply(lambda row: row['temp'][0:row['temp'].find('-')], axis=1)
    df[consolidacao.FONTE_RECURSOS_DESCRICAO] = df.apply(
        lambda row: row['temp'][row['temp'].find('-') + 1:len(row['temp'])], axis=1)
    df = df.drop(['temp'], axis=1)

    df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'
    return df


def __consolidar_portal_transparencia_estadual(data_extracao):
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Contratante',
                        consolidacao.CONTRATADO_DESCRICAO: 'Contratado(a)', consolidacao.CONTRATADO_CNPJ: 'CPF/ CNPJ',
                        consolidacao.DESPESA_DESCRICAO: 'Descrição do bem ou serviço',
                        consolidacao.ITEM_EMPENHO_QUANTIDADE: 'Qtde',
                        consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'Valor Global',
                        consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'Valor unitário',
                        consolidacao.MOD_APLIC_DESCRICAO: 'Forma/Modalidade da contratação',
                        consolidacao.DATA_CELEBRACAO: 'Data de celebração do contrato',
                        consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'Local da execuçao',
                        consolidacao.NUMERO_PROCESSO: 'N˚ Processo'}
    colunas_adicionais = ['Prazo Contratual', 'Contrato', 'DOE N˚']
    planilha_original = path.join(config.diretorio_dados, 'PA', 'portal_transparencia', 'covid.xlsx')
    df_original = pd.read_excel(planilha_original)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_PA
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'PA', '', data_extracao, pos_processar_portal_transparencia_estadual)
    return df


def __consolidar_portal_transparencia_capital(data_extracao):
    dicionario_dados = {consolidacao.DOCUMENTO_NUMERO: 'Empenho', consolidacao.UG_DESCRICAO: 'Unidade Gestora',
                        consolidacao.DESPESA_DESCRICAO: 'Objeto', consolidacao.FONTE_RECURSOS_DESCRICAO: 'Fonte',
                        consolidacao.CONTRATADO_DESCRICAO: 'Fornecedor', consolidacao.VALOR_CONTRATO: 'Valor',
                        consolidacao.DOCUMENTO_DATA: 'Data', consolidacao.SITUACAO: 'Situacao'}
    planilha_original = path.join(config.diretorio_dados, 'PA', 'portal_transparencia', 'Belem',
                                  'Prefeitura Municipal de Belém - Transparência COVID-19 - Despesas.xlsx')
    df_original = pd.read_excel(planilha_original, header=1)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Belem
    df = consolidar_layout([], df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL, fonte_dados, 'PA',
                           get_codigo_municipio_por_nome('Belém', 'PA'), data_extracao,
                           pos_processar_portal_transparencia_capital)
    return df


def __consolidar_tcm(data_extracao):
    # Salva arquivos adicionais (informações acessórias que podem ser relevantes)
    fornecedores_por_valor_homologado = pd.read_excel(os.path.join(config.diretorio_dados, 'PA', 'tcm',
                                                                   'Argus TCMPA - Fornecedores por Valor Homologado.xlsx'))
    fornecedores_por_valor_homologado[consolidacao.FONTE_DADOS] = config.url_tcm_PA_1
    fornecedores_por_valor_homologado[consolidacao.DATA_EXTRACAO_DADOS] = data_extracao
    salvar(fornecedores_por_valor_homologado, 'PA', '_fornecedores_por_valor_homologado')

    fornecedores_por_qtd_municipios = pd.read_excel(os.path.join(config.diretorio_dados, 'PA', 'tcm',
                                                                 'Argus TCMPA - Fornecedores por Quantidade de Municípios.xlsx'))
    fornecedores_por_qtd_municipios[consolidacao.FONTE_DADOS] = config.url_tcm_PA_2
    fornecedores_por_qtd_municipios[consolidacao.DATA_EXTRACAO_DADOS] = data_extracao
    salvar(fornecedores_por_qtd_municipios, 'PA', '_fornecedores_por_qtd_municipios')


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Pará')

    portal_transparencia_estadual = __consolidar_portal_transparencia_estadual(data_extracao)
    portal_transparencia_capital = __consolidar_portal_transparencia_capital(data_extracao)
    portal_transparencia_estadual = portal_transparencia_estadual.append(portal_transparencia_capital)

    salvar(portal_transparencia_estadual, 'PA')

    __consolidar_tcm(data_extracao)
