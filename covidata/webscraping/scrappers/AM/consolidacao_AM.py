import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pos_processar_materiais_capital(df):
    # Elimina as 17 últimas linhas, que só contêm totalizadores e legendas
    df.drop(df.tail(17).index, inplace=True)

    df[consolidacao.MUNICIPIO_DESCRICAO] = 'Manaus'
    df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
    df.loc[(pd.isna(df[consolidacao.CONTRATADO_CNPJ])), consolidacao.FAVORECIDO_TIPO] = ''
    df[consolidacao.TIPO_DOCUMENTO] = 'Empenho'

    return df


def consolidar_contratos(data_extracao):
    dicionario_dados = {consolidacao.UG_DESCRICAO: 'UG', consolidacao.CONTRATADO_CNPJ: 'CNPJ/CPFfornecedor',
                        consolidacao.CONTRATADO_DESCRICAO: 'Nomefornecedor', consolidacao.DESPESA_DESCRICAO: 'Objeto',
                        consolidacao.CONTRATANTE_DESCRICAO: 'UG', consolidacao.DATA_ASSINATURA: 'Dataassinatura',
                        consolidacao.DATA_INICIO_VIGENCIA: 'Início',
                        consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'Local deexecução',
                        consolidacao.DATA_FIM_VIGENCIA: 'Término'}
    colunas_adicionais = ['Termo', 'Motivação/Justificativa', 'Processo e-Compras', 'Valor mensal', 'Valor atual']
    df_original = pd.read_excel(
        path.join(config.diretorio_dados, 'AM', 'portal_transparencia',
                  'Portal SGC - Sistema de Gestão de Contratos.xlsx'))
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_AM, 'AM', '',
                           data_extracao)
    df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
    return df


def consolidar_materiais_capital(data_extracao):
    dicionario_dados = {consolidacao.UG_DESCRICAO: 'ÓRGÃO', consolidacao.CONTRATANTE_DESCRICAO: 'ÓRGÃO',
                        consolidacao.DESPESA_DESCRICAO: 'MATERIAL/SERVIÇO',
                        consolidacao.MOD_APLIC_DESCRICAO: 'MODALIDADE', consolidacao.ITEM_EMPENHO_QUANTIDADE: 'QTD',
                        consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'VLR UNITÁRIO',
                        consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'VLR  TOTAL POR ITEM',
                        consolidacao.VALOR_CONTRATO: 'VLR TOTAL CONTRATADO',
                        consolidacao.CONTRATADO_DESCRICAO: 'FORNECEDOR', consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                        consolidacao.DOCUMENTO_NUMERO: 'NOTA DE EMPENHO',
                        consolidacao.DATA_CELEBRACAO: 'DATA DA CELEBRAÇÃO DO CONTRATO',
                        consolidacao.PRAZO_EM_DIAS: 'PRAZO DE ENTREGA (EM DIAS)',
                        consolidacao.NUMERO_PROCESSO: 'PROCESSO'}
    colunas_adicionais = ['DESTINO', 'ID', 'PUBLICIDADE', 'EDITAL DE LICITAÇÃO', 'CONTRATO',
                          'MODALIDADE DA NOTA DE EMPENHO']
    df_original = pd.read_csv(
        path.join(config.diretorio_dados, 'AM', 'portal_transparencia', 'Manaus',
                  'PÚBLICA-CONTROLE-PROCESSOS-COMBATE-COVID-19-MATERIAIS-2.csv'))
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Manaus, 'AM',
                           get_codigo_municipio_por_nome('Manaus', 'AM'), data_extracao,
                           pos_processar_materiais_capital)
    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Amazonas')
    contratos = consolidar_contratos(data_extracao)

    materiais_capital = consolidar_materiais_capital(data_extracao)
    contratos = contratos.append(materiais_capital)

    salvar(contratos, 'AM')
