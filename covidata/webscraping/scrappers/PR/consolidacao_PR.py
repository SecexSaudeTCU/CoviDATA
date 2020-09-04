import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pos_processar_aquisicoes_capital(df):
    df[consolidacao.MUNICIPIO_DESCRICAO] = 'Curitiba'
    df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'

    # Como no caso de PR, em nenhum momento a informação de CNPJ é fornecida, sinalizar tipo para PJ para que, na
    # consolidação geral seja executada a busca por CNPJs.
    df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    return df


def pos_processar_licitacoes_capital(df):
    df = pos_processar_aquisicoes_capital(df)
    df[consolidacao.TIPO_DOCUMENTO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    # Como no caso de PR, em nenhum momento a informação de CNPJ é fornecida, sinalizar tipo para PJ para que, na
    # consolidação geral seja executada a busca por CNPJs.
    df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    return df


def __consolidar_aquisicoes_capital(data_extracao):
    dicionario_dados = {consolidacao.DOCUMENTO_DATA: 'Data', consolidacao.DOCUMENTO_NUMERO: 'Documento/Empenho',
                        consolidacao.FUNCAO_DESCRICAO: 'Função', consolidacao.SUBFUNCAO_DESCRICAO: 'Subfunção',
                        consolidacao.FONTE_RECURSOS_DESCRICAO: 'Fonte', consolidacao.MOD_APLIC_DESCRICAO: 'Modalidade',
                        consolidacao.ELEMENTO_DESPESA_DESCRICAO: 'Elemento',
                        consolidacao.CONTRATANTE_DESCRICAO: 'Órgão', consolidacao.UG_DESCRICAO: 'Órgão',
                        consolidacao.VALOR_CONTRATO: 'Valor R$',
                        consolidacao.CATEGORIA_ECONOMICA_DESCRICAO: 'Categoria',
                        consolidacao.GND_DESCRICAO: 'Grupo'}
    planilha_original = path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba', 'aquisicoes.xlsx')
    df_original = pd.read_excel(planilha_original, header=7)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Curitiba_aquisicoes
    codigo_municipio_ibge = get_codigo_municipio_por_nome('Curitiba', 'PR')
    df = consolidar_layout([], df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           fonte_dados, 'PR', codigo_municipio_ibge, data_extracao,
                           pos_processar_aquisicoes_capital)
    return df


def __consolidar_licitacoes_capital(data_extracao):
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'ÓRGÃO', consolidacao.UG_DESCRICAO: 'ÓRGÃO',
                        consolidacao.DESPESA_DESCRICAO: 'OBJETO',
                        consolidacao.MOD_APLIC_DESCRICAO: 'MODALIDADE DA CONTRATAÇÃO',
                        consolidacao.MOD_APLICACAO_COD: 'Nº DA MODALIDADE',
                        consolidacao.CONTRATADO_DESCRICAO: 'CONTRATADO (s)', consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                        consolidacao.ITEM_EMPENHO_QUANTIDADE: 'QUANTIDADE',
                        consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: ' VALOR UNITÁRIO ',
                        consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'VALOR TOTAL/GLOBAL',
                        consolidacao.VALOR_EMPENHADO: 'EMPENHADO', consolidacao.DOCUMENTO_NUMERO: 'EMPENHO Nº ',
                        consolidacao.VALOR_LIQUIDADO: 'VALOR LIQUIDADO', consolidacao.VALOR_PAGO: 'VALOR PAGO',
                        consolidacao.FONTE_RECURSOS_DESCRICAO: 'FONTE DE RECURSOS',
                        consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'LOCAL DA EXECUÇÃO/ENTREGA',
                        consolidacao.NUMERO_CONTRATO: 'Nº CONTRATO',
                        consolidacao.DATA_PUBLICACAO: 'PUBLICAÇÃO NO DIÁRIO OFICIAL'}
    colunas_adicionais = ['PROTOCOLO', 'VIGENCIA DO CONTRATO', 'VALOR CANCELADO', 'BOLETIM DE PAGAMENTO', 'A PAGAR']
    planilha_original = path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'Curitiba',
                                  'licitacoes_contratacoes.csv')
    df_original = pd.read_csv(planilha_original, sep=';', header=0, encoding='ISO-8859-1')
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Curitiba_contratacoes
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           fonte_dados, 'PR', get_codigo_municipio_por_nome('Curitiba', 'PR'), data_extracao,
                           pos_processar_licitacoes_capital)
    return df


def consolidar(data_extracao, df_consolidado):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Paraná')

    aquisicoes_capital = __consolidar_aquisicoes_capital(data_extracao)
    df_consolidado = df_consolidado.append(aquisicoes_capital)

    licitacoes_capital = __consolidar_licitacoes_capital(data_extracao)
    df_consolidado = df_consolidado.append(licitacoes_capital)

    salvar(df_consolidado, 'PR')
