import logging
from os import path

import pandas as pd

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pos_processar_aquisicoes(df):
    df = df.astype({consolidacao.CONTRATADO_CNPJ: str})

    for i in range(0, len(df)):
        df.loc[i, consolidacao.CONTRATADO_CNPJ] = df.loc[i, consolidacao.CONTRATADO_CNPJ].replace(',001', '')
        tamanho = len(df.loc[i, consolidacao.CONTRATADO_CNPJ])

        if tamanho < 14:
            df.loc[i, consolidacao.CONTRATADO_CNPJ] = '0' * (14 - tamanho) + df.loc[i, consolidacao.CONTRATADO_CNPJ]

    return df


def __consolidar_aquisicoes(data_extracao):
    dicionario_dados = {consolidacao.DESPESA_DESCRICAO: 'Objeto', consolidacao.ITEM_EMPENHO_QUANTIDADE: 'Qdt Item',
                        consolidacao.ITEM_EMPENHO_UNIDADE_MEDIDA: 'Unidade', consolidacao.VALOR_CONTRATO: 'Valor',
                        consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'Valor',
                        consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'Valor Item',
                        consolidacao.VALOR_EMPENHADO: 'Empenhado', consolidacao.VALOR_LIQUIDADO: 'Liquidado',
                        consolidacao.VALOR_PAGO: 'Pago', consolidacao.CONTRATADO_DESCRICAO: 'Credor',
                        consolidacao.CONTRATADO_CNPJ: 'CPF_CNPJ_COTACOES'}
    colunas_adicionais = ['Fase da Licitação', 'TR', 'Mapa de Preços', 'Contrato', 'Local de Execução',
                          'Data Solicitação', 'Natureza', 'Processo', 'Tempo de Contratação']
    planilha_original = path.join(config.diretorio_dados, 'GO', 'portal_transparencia', 'aquisicoes.csv')
    df_original = pd.read_csv(planilha_original)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_GO
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'GO', '', data_extracao, pos_processar_aquisicoes)
    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Goiás')

    aquisicoes = __consolidar_aquisicoes(data_extracao)

    salvar(aquisicoes, 'GO')
