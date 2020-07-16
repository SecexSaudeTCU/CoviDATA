import logging
import os
from os import path

import pandas as pd

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar
from covidata.municipios.ibge import get_codigo_municipio_por_nome


def pos_processar_despesas(df):
    df[consolidacao.TIPO_DOCUMENTO] = 'EMPENHO'

    for i in range(0, len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

        if len(cpf_cnpj) == 11:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
        elif len(cpf_cnpj) > 11:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    return df


def __consolidar_despesas(data_extracao):
    dicionario_dados = {consolidacao.CONTRATADO_CNPJ: 'CPF CNPJ CONTRATADO',
                        consolidacao.ELEMENTO_DESPESA_DESCRICAO: 'ELEMENTO DESPESA',
                        consolidacao.ITEM_EMPENHO_QUANTIDADE: 'QUANTIDADE', consolidacao.DESPESA_DESCRICAO: 'OBJETO',
                        consolidacao.MOD_APLIC_DESCRICAO: 'MODALIDADE CONTRATACAO',
                        consolidacao.CONTRATANTE_DESCRICAO: 'ORGAO CONTRATANTE',
                        consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'VALOR UNITARIO',
                        consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'VALOR TOTAL',
                        consolidacao.ITEM_EMPENHO_UNIDADE_MEDIDA: 'UNIDADE MEDIDA',
                        consolidacao.UG_COD: 'UG', consolidacao.CONTRATADO_DESCRICAO: 'NOME CONTRATADO',
                        consolidacao.DOCUMENTO_NUMERO: 'NOTA EMPENHO', consolidacao.UG_DESCRICAO: 'ORGAO CONTRATANTE',
                        consolidacao.VALOR_CONTRATO: 'VALOR TOTAL', consolidacao.DATA_CELEBRACAO: 'DATA CELEBRACAO',
                        consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'LOCAL ENTREGA',
                        consolidacao.PRAZO_EM_DIAS: 'PRAZO CONTRATUAL', consolidacao.NUMERO_CONTRATO: 'CONTRATO',
                        consolidacao.NUMERO_PROCESSO: 'PROCESSO'}
    df_original = pd.read_excel(
        path.join(config.diretorio_dados, 'AL', 'portal_transparencia', 'DESPESAS COM COVID-19.xls'), header=7)
    df = consolidar_layout([], df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_AL, 'AL', '',
                           data_extracao, pos_processar_despesas)
    return df


def __consolidar_compras_capital(data_extracao):
    dicionario_dados = {consolidacao.DESPESA_DESCRICAO: 'objeto', consolidacao.MOD_APLICACAO_COD: 'numero_modalidade',
                        consolidacao.ANO: 'ano_modalidade', consolidacao.MOD_APLIC_DESCRICAO: 'modalidade',
                        consolidacao.CONTRATANTE_DESCRICAO: 'orgao_nome', consolidacao.UG_DESCRICAO: 'orgao_nome',
                        consolidacao.NUMERO_PROCESSO: 'num_processo'}
    colunas_adicionais = ['data_abertura', 'hora_abertura', 'data_fechamento', 'hora_fechamento', 'orgao_sigla', 'cota',
                          'status', 'responsavel']
    df_original = pd.read_excel(
        path.join(config.diretorio_dados, 'AL', 'portal_transparencia', 'Maceio', 'compras.xlsx'))
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Maceio
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           fonte_dados, 'AL', get_codigo_municipio_por_nome('Maceió', 'AL'), data_extracao)
    df[consolidacao.MUNICIPIO_DESCRICAO] = 'Maceió'

    # Salva arquivos adicionais (informações acessórias que podem ser relevantes)
    planilha_original = os.path.join(config.diretorio_dados, 'AL', 'portal_transparencia', 'Maceio', 'compras.xlsx')

    documentos = pd.read_excel(planilha_original, sheet_name='documentos')
    documentos[consolidacao.FONTE_DADOS] = fonte_dados
    salvar(documentos, 'AL', '_Maceio_documentos')

    homologacoes = pd.read_excel(planilha_original, sheet_name='homologacoes')
    homologacoes[consolidacao.FONTE_DADOS] = fonte_dados
    salvar(homologacoes, 'AL', '_Maceio_homologacoes')

    atas = pd.read_excel(planilha_original, sheet_name='atas')
    atas[consolidacao.FONTE_DADOS] = fonte_dados
    salvar(atas, 'AL', '_Maceio_atas')

    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Alagoas')
    despesas = __consolidar_despesas(data_extracao)

    compras_capital = __consolidar_compras_capital(data_extracao)
    despesas = despesas.append(compras_capital)

    salvar(despesas, 'AL')
