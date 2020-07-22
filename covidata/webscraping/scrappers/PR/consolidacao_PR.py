import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pos_processar_dados_abertos(df):
    df[consolidacao.ANO] += 2000
    df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'

    # Como no caso de PR, em nenhum momento a informação de CNPJ é fornecida, sinalizar tipo para PJ para que, na
    # consolidação geral seja executada a busca por CNPJs.
    df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    return df


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


def __consolidar_aquisicoes(data_extracao, url_fonte_dados):
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'ORGÃO - CONTRATANTE/MUNICÍPIO',
                        consolidacao.UG_DESCRICAO: 'ORGÃO - CONTRATANTE/MUNICÍPIO',
                        consolidacao.CONTRATADO_DESCRICAO: 'EMPRESA CONTRATADA/CNPJ ',
                        consolidacao.DESPESA_DESCRICAO: 'OBJETO',
                        consolidacao.ITEM_EMPENHO_QUANTIDADE: 'QUANTIDADE POR UNIDADES / DIÁRIAS',
                        consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'VALOR    UNITÁRIO (R$)',
                        consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'VALOR                          TOTAL (R$)',
                        consolidacao.DATA_PUBLICACAO: 'DATA  PUBLICAÇÃO                       DIOE / FOLHA'}
    colunas_adicionais = ['PRAZO              CONTRATO (mês)', 'NÚMERO DISPENSA', 'PROTOCOLO / PROCESSO']
    planilha_original = path.join(config.diretorio_dados, 'PR', 'portal_transparencia',
                                  'aquisicoes_e_contratacoes_0.xls')
    df_original = pd.read_excel(planilha_original, header=6, sheet_name='UNIFICAÇÃO DOS 4 MESES')
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + url_fonte_dados
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'PR', '', data_extracao)

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


def __consolidar_dados_abertos(data_extracao, url_fonte_dados):
    dicionario_dados = {consolidacao.ANO: 'ANO', consolidacao.DOCUMENTO_NUMERO: 'EMPENHO',
                        consolidacao.FUNCAO_COD: 'FUNÇÃO', consolidacao.SUBFUNCAO_COD: 'SUBFUNÇÃO',
                        consolidacao.PROGRAMA_COD: 'PROGRAMA', consolidacao.MOD_APLICACAO_COD: 'MODALIDADE',
                        consolidacao.ELEMENTO_DESPESA_COD: 'ELEMENTO',
                        consolidacao.SUB_ELEMENTO_DESPESA_COD: 'SUBELEMENTO', consolidacao.FONTE_RECURSOS_COD: 'FONTE',
                        consolidacao.VALOR_EMPENHADO: 'EMPENHADO', consolidacao.VALOR_LIQUIDADO: 'LIQUIDADO',
                        consolidacao.VALOR_PAGO: 'PAGO', consolidacao.CATEGORIA_ECONOMICA_COD: 'CATEGORIA',
                        consolidacao.CONTA_CORRENTE: 'CONTA CORRENTE', consolidacao.ESPECIE: 'ESPECIE'}
    colunas_adicionais = ['MÊS', 'PODER', 'UNIDADE CONTÁBIL', 'UNIDADE ORÇAMENTÁRIA', 'P_A_OE', 'NATUREZA', 'NATUREZA2',
                          'OBRA_META', 'HISTORICO', 'Modalidade2']
    planilha_original = path.join(config.diretorio_dados, 'PR', 'portal_transparencia', 'dados_abertos.xlsx')
    df_original = pd.read_excel(planilha_original)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + url_fonte_dados
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'PR', '', data_extracao, pos_processar_dados_abertos)
    return df


def consolidar(data_extracao, url_fonte_aquisicoes, url_fonte_dados_abertos):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Paraná')

    aquisicoes = __consolidar_aquisicoes(data_extracao, url_fonte_aquisicoes)

    dados_abertos = __consolidar_dados_abertos(data_extracao, url_fonte_dados_abertos)
    aquisicoes = aquisicoes.append(dados_abertos)

    aquisicoes_capital = __consolidar_aquisicoes_capital(data_extracao)
    aquisicoes = aquisicoes.append(aquisicoes_capital)

    licitacoes_capital = __consolidar_licitacoes_capital(data_extracao)
    aquisicoes = aquisicoes.append(licitacoes_capital)

    salvar(aquisicoes, 'PR')
