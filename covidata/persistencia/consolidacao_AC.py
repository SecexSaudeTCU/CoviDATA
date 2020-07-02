from os import path

import numpy as np
import pandas as pd

from covidata import config
from covidata.persistencia import consolidacao
from covidata.persistencia.consolidacao import consolidar


def pos_processar(df_original, df_despesas, df_itens_empenho):
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


def main():
    dicionario_dados = {consolidacao.EMPENHO_NUMERO: '\nNUMEROEMPENHO\n',
                        consolidacao.FAVORECIDO_DESCRICAO: '\nRazão Social\n',
                        consolidacao.FAVORECIDO_COD: '\nCPF/CNPJ\n', consolidacao.EMPENHO_DATA: '\nData do Empenho\n',
                        consolidacao.FONTE_RECURSOS_COD: '\nFonte de Recurso\n',
                        consolidacao.VALOR_EMPENHADO: '\nValor Empenhado\r\n  ($)\n'}
    colunas_adicionais_despesas = ['\nTipo de Credor\n']
    tipo_fonte = 'tce'
    # TODO: Sugerir uma nova coluna 'TIPO_FONTE'
    fonte_dados = consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_despesas
    # TODO: Nem sempre esta informação está presente
    ano = consolidacao.ANO_PADRAO
    uf = 'AC'
    esfera = consolidacao.ESFERA_ESTADUAL
    codigo_municipio_ibge = ''
    nome_arquivo = 'despesas.xls'
    df = pd.read_excel(path.join(config.diretorio_dados, uf, tipo_fonte, nome_arquivo), header=4)
    df_despesas, _ = consolidar(ano, colunas_adicionais_despesas, df, dicionario_dados, esfera, fonte_dados, uf,
                                codigo_municipio_ibge, pos_processar)
    df_despesas.to_excel(path.join(config.diretorio_dados, 'consolidados', 'TCE_AC_despesas.xlsx'))


main()
