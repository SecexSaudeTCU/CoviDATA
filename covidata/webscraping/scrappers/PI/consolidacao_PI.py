import logging
from os import path

import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar



def pre_processar_tce(df):

    # Renomeia colunas do objeto pandas DataFrame "df"
    df.rename(index=str,
              columns={'processo tce': 'Número Processo TCE',
                       'instrumento': 'Tipo Instrumento',
                       'nº/ano': 'Número Instrumento',
                       'procedimento': 'Tipo Procedimento',
                       'tipo contrato': 'Tipo Contratual',
                       'status': 'Status Contratual',
                       'dt ini vig atual': 'Data Início Vigência',
                       'dt fim vig atual': 'Data Fim Vigência',
                       'Data última atualização': 'Data Última Atualização'},
              inplace=True)

    return df


def pos_processar_tce(df):

    for i in range(len(df)):
        cpf_cnpj = df.loc[str(i), consolidacao.CONTRATADO_CNPJ]

        if len(cpf_cnpj) >= 14:
            df.loc[str(i), consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        else:
            df.loc[str(i), consolidacao.FAVORECIDO_TIPO] = 'CPF/RG'

    return df


def consolidar_tce(data_extracao):

    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.CONTRATANTE_DESCRICAO: 'órgão',
                        consolidacao.DESPESA_DESCRICAO: 'objeto',
                        consolidacao.VALOR_CONTRATO: 'valor',
                        consolidacao.CONTRATADO_DESCRICAO: 'contratada',
                        consolidacao.CONTRATADO_CNPJ: 'doc contratada'}

    # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
    colunas_adicionais = ['Número Processo TCE', 'Tipo Instrumento', 'Número Instrumento',
                          'Tipo Procedimento', 'Tipo Contratual', 'Status Contratual',
                          'Data Assinatura', 'Data Início Vigência', 'Data Fim Vigência',
                          'Data Cadastro', 'Data Última Atualização']

    # Lê o arquivo "xlsx" de contratos baixado como um objeto pandas DataFrame

    df_original = pd.read_excel(path.join(config.diretorio_dados, 'PI', 'tce', 'contratos.xlsx'))

    # Chama a função "pre_processar_tce" definida neste módulo
    df = pre_processar_tce(df_original)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(colunas_adicionais, df, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_tce_PI, 'PI', '',
                           data_extracao, pos_processar_tce)

    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Piauí')

    consolidacoes = consolidar_tce(data_extracao)

    salvar(consolidacoes, 'PI')
