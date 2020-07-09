import logging
from os import path

import pandas as pd

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def __consolidar_dados_portal_transparencia(data_extracao):
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'ÓRGÃO', consolidacao.UG_DESCRICAO: 'ÓRGÃO',
                        consolidacao.CONTRATADO_DESCRICAO: 'CONTRATADO', consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                        consolidacao.DESPESA_DESCRICAO: 'OBJETO', consolidacao.ITEM_EMPENHO_QUANTIDADE: 'QUANTIDADE',
                        consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'VALOR UNITÁRIO',
                        consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'VALOR TOTAL',
                        consolidacao.VALOR_CONTRATO: 'VALOR TOTAL'}
    colunas_adicionais = ['INSTRUMENTO CONTRATUAL', 'PROCESSO', 'CELEBRAÇÃO', 'VIGÊNCIA', 'LOCAL ENTREGA/EXECUÇÃO',
                          'PUBLICAÇÃO DODF', 'PORTAL COVID-19', 'link_convenio', 'link_contrato', 'link_processo',
                          'link_plano_de_trabalho', 'link_justificativa', 'lik_edital_credenciamento',
                          'link_proposta_empresa', 'link_mapa_precos', 'link_projeto_basico',
                          'link_planilha_consolidada', 'link_termo_aditivo', 'link_termo_de_referência',
                          'link_nota_tecnica', 'link_termo_colob_emerg']
    for i in range(1, 11):
        colunas_adicionais.append(f'NE{i}')
        colunas_adicionais.append(f'pdfNE{i}')
        colunas_adicionais.append(f'NE{i}Cancelada')

    planilha_original = path.join(config.diretorio_dados, 'DF', 'portal_transparencia', 'DistritoFederal',
                                  'Dados_Portal_Transparencia_DistritoFederal.xlsx')
    df_original = pd.read_excel(planilha_original)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_DF
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'DF', '', data_extracao)
    df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Distrito Federal')

    dados = __consolidar_dados_portal_transparencia(data_extracao)
    salvar(dados, 'DF')

