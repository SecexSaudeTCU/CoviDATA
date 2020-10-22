import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pre_processar_pt_SC_contratos(df):
    # Renomeia as colunas especificadas
    df.rename(index=str,
              columns={'CDGESTAO': 'Código Gestão',
                       'NMGESTAOCONTRATANTE': 'Nome Gestão Contratante',
                       'NMMODALIDADE': 'Modalidade Licitação',
                       'CDITEMAPRESENTACAO': 'Código Item',
                       'DEITEM': 'Descrição Item',
                       'NMMARCA': 'Marca Item',
                       'DEDESCRICAO': 'Descrição Detalhada Item',
                       'QTITEM': 'Quantidade Item',
                       'VLUNITARIO': 'PU Item',
                       'VLINFORMADO': 'Preço Item',
                       'SGUNIDADEMEDIDA': 'Unidade Item',
                       'NUSERVICOMATERIAL': 'Número Serviço/Material',
                       'DESUBITEM': 'Descrição Subitem',
                       'VLUNITARIOSUBITEM': 'PU Subitem',
                       'QTSUBITEM': 'Quantidade Subitem',
                       'VLINFORMADOSUBITEM': 'Preço Subitem',
                       'SGUNIDADEMEDIDASUBITEM': 'Unidade Subitem',
                       'SIGLAORGAO': 'Sigla Órgão'},
              inplace=True)

    return df


def pre_processar_pt_SC_despesas(df):
    # Renomeia as colunas especificadas
    df.rename(index=str,
              columns={'vldotacaoinicial': 'Valor Dotação Inicial',
                       'vldotacaoatualizada': 'Valor Dotação Atualizada'},
              inplace=True)

    return df


def pre_processar_pt_Florianopolis(df):
    # Renomeia as colunas especificadas
    df.rename(index=str,
              columns={'Número Dispensa de Licitação': 'Número Dispensa',
                       'Local de Entrega da Prestação de Serviço': 'Local Entrega',
                       'Unidade': 'Unidade Objeto',
                       'Quantidade': 'Quantidade Objeto',
                       'Data de Assinatura Instumento Contratual': 'Data Assinatura Contrato',
                       'Processo de Contratação ou Aquisição  para Download': 'Número Processo',
                       'Instumento Contratual': 'Número Contrato',
                       'Modalidade de Licitação': 'Modalidade Licitação'},
              inplace=True)

    return df


def pos_processar_pts(df):
    for i in range(len(df)):
        cpf_cnpj = df.loc[str(i), consolidacao.CONTRATADO_CNPJ]

        if len(str(cpf_cnpj)) >= 14:
            df.loc[str(i), consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        else:
            df.loc[str(i), consolidacao.FAVORECIDO_TIPO] = 'CPF/RG'

    return df


def consolidar_pt_SC_contratos(data_extracao):
    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.NUMERO_CONTRATO: 'NUCONTRATO',
                        consolidacao.UG_COD: 'CDUNIDADEGESTORA',
                        consolidacao.UG_DESCRICAO: 'NMUGCONTRATANTE',
                        consolidacao.CONTRATADO_CNPJ: 'NUIDENTIFICACAOCONTRATADO',
                        consolidacao.CONTRATADO_DESCRICAO: 'NMCONTRATADO',
                        consolidacao.DESPESA_DESCRICAO: 'DEOBJETOCONTRATO',
                        consolidacao.NUMERO_PROCESSO: 'NUPROCESSO',
                        consolidacao.VALOR_CONTRATO: 'VLTOTALATUAL',
                        consolidacao.DATA_ASSINATURA: 'DTASSINATURA',
                        consolidacao.DATA_INICIO_VIGENCIA: 'DTINIVIGENCIA',
                        consolidacao.DATA_FIM_VIGENCIA: 'DTFIMVIGENCIAATUAL',
                        consolidacao.PRAZO_EM_DIAS: 'NUPRAZO',
                        consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'NMLOCALEXECUCAO',
                        consolidacao.FONTE_RECURSOS_COD: 'CDFONTERECURSO',
                        consolidacao.FONTE_RECURSOS_DESCRICAO: 'NMFONTERECURSO',
                        consolidacao.ORGAO_COD: 'CDORGAO',
                        consolidacao.CONTRATANTE_DESCRICAO: 'NMORGAO'}

    # Lê o arquivo "xlsx" de contratos baixado como um objeto pandas DataFrame
    df_original = pd.read_excel(path.join(config.diretorio_dados, 'SC', 'portal_transparencia',
                                          'contrato_item.xlsx'))

    # Chama a função "pre_processar_pt_SC_contratos" definida neste módulo
    df = pre_processar_pt_SC_contratos(df_original)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_SC_contratos, 'SC', '',
                           data_extracao, pos_processar_pts)

    return df


def consolidar_pt_SC_despesas(data_extracao):
    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.ORGAO_COD: 'codigoexibicao',
                        consolidacao.CONTRATANTE_DESCRICAO: 'descricao',
                        consolidacao.VALOR_EMPENHADO: 'vlempenhado',
                        consolidacao.VALOR_LIQUIDADO: 'vlliquidado',
                        consolidacao.VALOR_PAGO: 'vlpago'}

    # Lê o arquivo "csv" de despesas baixado como um objeto pandas DataFrame
    df_original = pd.read_csv(path.join(config.diretorio_dados, 'SC', 'portal_transparencia',
                                        'analisedespesa.csv'),
                              sep=';',
                              encoding='iso-8859-1')

    # Chama a função "pre_processar_pt_SC_despesas" definida neste módulo
    df = pre_processar_pt_SC_despesas(df_original)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_SC_despesas, 'SC', '',
                           data_extracao)

    return df


def consolidar_pt_Florianopolis(data_extracao):
    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'Órgão Contratante',
                        consolidacao.CONTRATADO_DESCRICAO: 'Nome do Contratado',
                        consolidacao.CONTRATADO_CNPJ: 'CPF/CNPJ do Contratado',
                        consolidacao.CONTRATADO_CNPJ: 'CPF/CNPJ do Contratado',
                        consolidacao.DESPESA_DESCRICAO: 'Objeto',
                        consolidacao.VALOR_CONTRATO: 'Valor Global',
                        consolidacao.DATA_ASSINATURA: 'Data Assinatura Contrato'}

    # Lê o arquivo "csv" de despesas baixado como um objeto pandas DataFrame
    df_original = pd.read_csv(path.join(config.diretorio_dados, 'SC', 'portal_transparencia',
                                        'Florianopolis', 'aquisicoes.csv'),
                              sep=';',
                              encoding='iso-8859-1')

    # Chama a função "pre_processar_pt_Florianopolis" definida neste módulo
    df = pre_processar_pt_Florianopolis(df_original)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(df, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Florianopolis, 'SC',
                           get_codigo_municipio_por_nome('Florianópolis', 'SC'), data_extracao, pos_processar_pts)

    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Santa Catarina')

    consolidacoes = consolidar_pt_SC_contratos(data_extracao)
    consolidacao_pt_SC_despesas = consolidar_pt_SC_despesas(data_extracao)
    consolidacao_pt_Florianopolis = consolidar_pt_Florianopolis(data_extracao)

    consolidacoes = consolidacoes.append(consolidacao_pt_SC_despesas, ignore_index=True, sort=False)
    consolidacoes = consolidacoes.append(consolidacao_pt_Florianopolis, ignore_index=True, sort=False)

    salvar(consolidacoes, 'SC')
