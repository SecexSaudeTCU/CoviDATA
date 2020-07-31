import logging
from os import path
from glob import glob
from datetime import datetime

import json
import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar_layout, salvar


def pre_processar_pt_despesas_Goiania(json):
    # Inicializa objetos list para armazenar dados contidos no "dicionário" "json"
    col_empenho = []
    col_data_empenho = []
    col_natureza_despesa = []
    col_cnpj = []
    col_nome_favorecido = []
    col_nome_orgao= []
    col_valor_empenhado = []
    col_valor_liquidado = []
    col_valor_pago = []

    # Aloca os valores das referidas chaves do "dicionário" "json" aos respectivos objetos list
    for i in range(len(json)):
        col_empenho.append(json[i]['Empenho'])
        col_data_empenho.append(json[i]['DataEmpenho'])
        col_natureza_despesa.append(json[i]['NaturezaDespesa'])
        col_cnpj.append(json[i]['CNPJ'])
        col_nome_favorecido.append(json[i]['NmFavorecido'])
        col_nome_orgao.append(json[i]['NmOrgao'])
        col_valor_empenhado.append(json[i]['VlEmpenhado'])
        col_valor_liquidado.append(json[i]['VlLiquidado'])
        col_valor_pago.append(json[i]['VlPago'])

    # Armazena os dados dos objetos list como um objeto pandas DataFrame
    df = pd.DataFrame(list(zip(col_empenho, col_data_empenho, col_natureza_despesa, col_cnpj,
                               col_nome_favorecido, col_nome_orgao, col_valor_empenhado,
                               col_valor_liquidado, col_valor_pago)),
                      columns=['Empenho', 'Data Empenho', 'Natureza Despesa', 'CNPJ',
                               'Nome Favorecido', 'Nome Órgão', 'Valor Empenhado',
                               'Valor Liquidado', 'Valor Pago'])

    # Slice da coluna de string "Data Empenho" apenas os caracteres de data (10 primeiros), em seguida...
    # os converte para datatime, em seguida para date apenas e, por fim, para string no formato "dd/mm/aaaa"
    df['Data Empenho'] = \
        df['Data Empenho'].apply(lambda x: datetime.strptime(x[:10], '%Y-%m-%d').date().strftime('%d/%m/%Y'))

    return df


def pos_processar_aquisicoes(df):
    df = df.astype({consolidacao.CONTRATADO_CNPJ: str})
    df[consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    for i in range(0, len(df)):
        df.loc[i, consolidacao.CONTRATADO_CNPJ] = df.loc[i, consolidacao.CONTRATADO_CNPJ].replace(',001', '')
        tamanho = len(df.loc[i, consolidacao.CONTRATADO_CNPJ])

        if tamanho < 14:
            df.loc[i, consolidacao.CONTRATADO_CNPJ] = '0' * (14 - tamanho) + df.loc[i, consolidacao.CONTRATADO_CNPJ]

    return df


def pos_processar_pt_despesas_Goiania(df):
    for i in range(len(df)):
        cpf_cnpj = df.loc[i, consolidacao.CONTRATADO_CNPJ]

        if len(str(cpf_cnpj)) >= 14:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ
        else:
            df.loc[i, consolidacao.FAVORECIDO_TIPO] = 'CPF/RG'

    return df


def __consolidar_aquisicoes(data_extracao):
    dicionario_dados = {consolidacao.DESPESA_DESCRICAO: 'Objeto', consolidacao.ITEM_EMPENHO_QUANTIDADE: 'Qdt Item',
                        consolidacao.ITEM_EMPENHO_UNIDADE_MEDIDA: 'Unidade', consolidacao.VALOR_CONTRATO: 'Valor',
                        consolidacao.ITEM_EMPENHO_VALOR_TOTAL: 'Valor',
                        consolidacao.ITEM_EMPENHO_VALOR_UNITARIO: 'Valor Item',
                        consolidacao.VALOR_EMPENHADO: 'Empenhado', consolidacao.VALOR_LIQUIDADO: 'Liquidado',
                        consolidacao.VALOR_PAGO: 'Pago', consolidacao.CONTRATADO_DESCRICAO: 'Credor',
                        consolidacao.CONTRATADO_CNPJ: 'CPF_CNPJ_COTACOES',
                        consolidacao.LOCAL_EXECUCAO_OU_ENTREGA: 'Local de Execução',
                        consolidacao.NUMERO_PROCESSO: 'Processo'}
    colunas_adicionais = ['Fase da Licitação', 'TR', 'Mapa de Preços', 'Contrato', 'Data Solicitação', 'Natureza',
                          'Tempo de Contratação']
    planilha_original = path.join(config.diretorio_dados, 'GO', 'portal_transparencia', 'aquisicoes.csv')
    df_original = pd.read_csv(planilha_original)
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_GO
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_ESTADUAL,
                           fonte_dados, 'GO', '', data_extracao, pos_processar_aquisicoes)
    return df


def __consolidar_pt_despesas_Goiania(data_extracao):
    # Objeto dict em que os valores tem chaves que retratam campos considerados mais importantes
    dicionario_dados = {consolidacao.DOCUMENTO_NUMERO: 'Empenho',
                        consolidacao.DOCUMENTO_DATA: 'Data Empenho',
                        consolidacao.ELEMENTO_DESPESA_DESCRICAO: 'Natureza Despesa',
                        consolidacao.CONTRATADO_CNPJ: 'CNPJ',
                        consolidacao.CONTRATADO_DESCRICAO: 'Nome Favorecido',
                        consolidacao.CONTRATANTE_DESCRICAO: 'Órgão',
                        consolidacao.VALOR_EMPENHADO: 'Valor Empenhado',
                        consolidacao.VALOR_LIQUIDADO: 'Valor Liquidado',
                        consolidacao.VALOR_PAGO: 'Valor Pago'}

    # Objeto list cujos elementos retratam campos não considerados tão importantes (for now at least)
    colunas_adicionais = []

    # Obtém objeto list dos arquivos armazenados no path passado como argumento para a função nativa "glob"
    list_files = glob(path.join(config.diretorio_dados, 'GO', 'portal_transparencia', 'Goiania/*'))

    # Obtém o primeiro elemento do objeto list que corresponde ao nome do arquivo "json" baixado
    file_name = list_files[0]

    # Armazena o conteúdo do arquivo "json" de nome "file_name" de despesas em um file handler
    f = open(file_name)

    # Carrega os dados no formato json
    data_original = json.load(f)

    # Chama a função "pre_processar_pt_despesas_Goiania" definida neste módulo
    df = pre_processar_pt_despesas_Goiania(data_original)

    # Chama a função "consolidar_layout" definida em módulo importado
    df = consolidar_layout(colunas_adicionais, df, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Goiania_despesas, 'GO',
                           get_codigo_municipio_por_nome('Goiânia', 'GO'), data_extracao, pos_processar_pt_despesas_Goiania)

    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Goiás')

    df = __consolidar_aquisicoes(data_extracao)
    consolidacoes_pt_despesas_Goiania = __consolidar_pt_despesas_Goiania(data_extracao)

    df = df.append(consolidacoes_pt_despesas_Goiania, ignore_index=True, sort=False)

    salvar(df, 'GO')

#consolidar(datetime.now())