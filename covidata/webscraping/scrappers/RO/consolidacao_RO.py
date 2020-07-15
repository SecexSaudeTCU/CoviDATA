import datetime
import logging

from covidata import config
from covidata.persistencia import consolidacao
from os import path
import pandas as pd

from covidata.persistencia.consolidacao import consolidar_layout, salvar
import numpy as np

def pos_consolidar_despesas(df):
    # Remove notação científica
    df = df.astype({consolidacao.CONTRATADO_CNPJ: np.uint64})
    df = df.astype({consolidacao.CONTRATADO_CNPJ: str})

    df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'

    return  df

def __consolidar_despesas(data_extracao):
    dicionario_dados = {consolidacao.ANO: 'Exercicio', consolidacao.DOCUMENTO_DATA: 'DataDocumento',
                        consolidacao.UG_COD: 'UG', consolidacao.PROGRAMA_COD: 'CodPrograma',
                        consolidacao.PROGRAMA_DESCRICAO: 'Programa', consolidacao.ACAO_COD: 'CodAco',
                        consolidacao.ACAO_DESCRICAO: 'Acao', consolidacao.FUNCAO_COD: 'CodFuncao',
                        consolidacao.FUNCAO_DESCRICAO: 'NomFuncao', consolidacao.SUBFUNCAO_COD: 'CodSubFuncao',
                        consolidacao.DESPESA_DESCRICAO: 'NomDespesa', consolidacao.FONTE_RECURSOS_COD: 'Fonte',
                        consolidacao.FONTE_RECURSOS_DESCRICAO: 'DesFonteRecurso',
                        consolidacao.CONTRATANTE_DESCRICAO: 'NomOrgao', consolidacao.UG_DESCRICAO: 'NomOrgao',
                        consolidacao.DOCUMENTO_NUMERO: 'documento', consolidacao.CONTRATADO_CNPJ: 'DocCredor',
                        consolidacao.CONTRATADO_DESCRICAO: 'Credor',
                        consolidacao.SUBFUNCAO_DESCRICAO: 'DescricaoSubfuncao',
                        consolidacao.VALOR_EMPENHADO: 'ValorEmpenhada', consolidacao.VALOR_LIQUIDADO: 'ValorLiquidada',
                        consolidacao.VALOR_PAGO: 'ValorPaga'}
    colunas_adicionais = ['CodProjeto', 'EVENTO', 'N_PROCESSO_NE', 'CodEspecificacaoDespesa', 'CodOrgao', 'NomSigla',
                          'NumEmpenho', 'DOCUMENT_NE', 'VLR_EMPENHO', 'valorDespesa', 'Status']
    planilha_original = path.join(config.diretorio_dados, 'RO', 'portal_transparencia', 'Despesas.CSV')
    df_original = pd.read_csv(planilha_original, sep=';', header=0, encoding='utf_16_le')
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_RO
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'RO', '', data_extracao, pos_consolidar_despesas)
    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Rondônia')

    despesas = __consolidar_despesas(data_extracao)

    salvar(despesas, 'RO')


consolidar(datetime.datetime.now())
