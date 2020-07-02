from os import path

import numpy as np
import pandas as pd

from covidata import config
from covidata.municipios.ibge import get_municipios_por_uf
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar


def pos_processar_despesas_uf(df_original, df_despesas, df_itens_empenho):
    # Elimina a última linha, que só contém um totalizador
    df_despesas = df_despesas.drop(df_despesas.index[-1])
    df_despesas = df_despesas.astype({consolidacao.FAVORECIDO_COD: np.uint64})
    df_despesas = df_despesas.astype({consolidacao.FAVORECIDO_COD: str})
    df_despesas = df_despesas.astype({consolidacao.EMPENHO_NUMERO: np.uint64})
    df_despesas = df_despesas.astype({consolidacao.EMPENHO_NUMERO: str})
    df_despesas.fillna('')

    df = df_original.fillna('NA')
    df = df[['\nCPF/CNPJ\n']].astype(str)

    for i in range(0, len(df)):
        cpf_cnpj = df.loc[i, '\nCPF/CNPJ\n']

        if len(cpf_cnpj) == 11:
            df_despesas.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
        elif len(cpf_cnpj) > 11:
            df_despesas.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    return df_despesas, df_itens_empenho


def pos_processar_despesas_municipio(df_original, df_despesas, df_itens_empenho):
    # Elimina a última linha, que só contém um totalizador
    df_despesas = df_despesas.drop(df_despesas.index[-1])
    df_despesas.fillna('')

    df = df_original.fillna('NA')
    df = df[['\nCNPJ/CPF\n']].astype(str)

    for i in range(0, len(df)):
        cpf_cnpj = df.loc[i, '\nCNPJ/CPF\n'].strip()

        if len(cpf_cnpj) == 14:
            df_despesas.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
        elif len(cpf_cnpj) > 14:
            df_despesas.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

    codigos_municipios = get_municipios_por_uf('AC')

    # Define os municípios
    df_despesas[consolidacao.COD_MUNICIPIO_IBGE] = df_despesas.apply(
        lambda row: codigos_municipios.get(__get_nome_municipio(row), ''), axis=1)

    # df_despesas = df_despesas.astype({consolidacao.COD_MUNICIPIO_IBGE: np.uint64})
    df_despesas = df_despesas.astype({consolidacao.COD_MUNICIPIO_IBGE: str})

    return df_despesas, df_itens_empenho


def __get_nome_municipio(row):
    string_original = row[consolidacao.ORGAO_DESCRICAO]
    prefixo = '\nPrefeitura Municipal de '
    nome_municipio = string_original[len(prefixo):len(string_original)].strip()
    return nome_municipio


def consolidar_despesas_uf():
    dicionario_dados = {consolidacao.EMPENHO_NUMERO: '\nNUMEROEMPENHO\n',
                        consolidacao.FAVORECIDO_DESCRICAO: '\nRazão Social\n',
                        consolidacao.FAVORECIDO_COD: '\nCPF/CNPJ\n', consolidacao.EMPENHO_DATA: '\nData do Empenho\n',
                        consolidacao.FONTE_RECURSOS_COD: '\nFonte de Recurso\n',
                        consolidacao.VALOR_EMPENHADO: '\nValor Empenhado\r\n  ($)\n'}
    # TODO: Sugerir uma nova coluna 'TIPO_FONTE'
    # TODO: Nem sempre esta informação está presente
    df = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'despesas.xls'), header=4)
    df_despesas, _ = consolidar(consolidacao.ANO_PADRAO, ['\nTipo de Credor\n'], df, dicionario_dados,
                                consolidacao.ESFERA_ESTADUAL,
                                consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_despesas, 'AC', '',
                                pos_processar_despesas_uf)
    return df_despesas


def consolidar_despesas_municipios():
    dicionario_dados = {consolidacao.ORGAO_DESCRICAO: '\nPREFEITURAS MUNICIPAIS NO ESTADO DO ACRE\n',
                        consolidacao.UG_DESCRICAO: '\nPREFEITURAS MUNICIPAIS NO ESTADO DO ACRE\n',
                        consolidacao.FAVORECIDO_COD: '\nCNPJ/CPF\n'}
    colunas_adicionais_despesas = ['\nCONTRATOS/OBSERVAÇÕES\n', '\n VALOR CONTRATADO R$\n']

    df = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'despesas_municipios.xls'), header=4)
    df_despesas, _ = consolidar(consolidacao.ANO_PADRAO, colunas_adicionais_despesas, df, dicionario_dados,
                                consolidacao.ESFERA_MUNICIPAL,
                                consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_despesas_municipios, 'AC', '',
                                pos_processar_despesas_municipio)
    return df_despesas


def main():
    # TODO: Pode ser recomendável unificar os formatos de CPF e CNPJ para remover pontos e hífens.
    despesas_uf = consolidar_despesas_uf()
    despesas_municipios = consolidar_despesas_municipios()
    despesas_uf = despesas_uf.append(despesas_municipios)
    despesas_uf.fillna('')
    despesas_uf.to_excel(path.join(config.diretorio_dados, 'consolidados', 'TCE_AC_despesas.xlsx'))
