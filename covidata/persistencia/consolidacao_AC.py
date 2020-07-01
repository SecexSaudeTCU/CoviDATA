from covidata.persistencia import consolidacao
from covidata import config
import datetime
import pandas as pd
from os import path

dicionario_dados = {consolidacao.EMPENHO_NUMERO: '\nNUMEROEMPENHO\n',
                    consolidacao.FAVORECIDO_DESCRICAO: '\nRazão Social\n',
                    consolidacao.FAVORECIDO_COD: '\nCPF/CNPJ\n', consolidacao.EMPENHO_DATA: '\nData do Empenho\n',
                    consolidacao.FONTE_RECURSOS_COD: '\nFonte de Recurso\n',
                    consolidacao.VALOR_EMPENHADO: '\nValor Empenhado\r\n  ($)\n'}
colunas_adicionais_despesas = ['\nTipo de Credor\n']

# TODO: Sugerir uma nova coluna 'TIPO_FONTE'
fonte_dados = consolidacao.TIPO_FONTE_TCE + ' - ' + config.url_tce_AC_despesas

# TODO: Incluir rotina que recupera a data de extração a partir de um arquivo texto
data_extracao = datetime.datetime.now()

# TODO: Nem sempre esta informação está presente
ano = consolidacao.ANO_PADRAO

uf = 'AC'

esfera = consolidacao.ESFERA_ESTADUAL

codigo_municipio_ibge = 'NA'

df = pd.read_excel(path.join(config.diretorio_dados, 'AC', 'tce', 'despesas.xlsx'))

df_despesas, df_itens_empenho = consolidacao.consolidar(df, dicionario_dados, colunas_adicionais_despesas, [], uf,
                                                        fonte_dados, data_extracao, ano, esfera)
df_despesas = df_despesas.fillna(0)
df_despesas = df_despesas.astype(
     {consolidacao.FONTE_RECURSOS_COD: int, consolidacao.EMPENHO_NUMERO: int, consolidacao.FAVORECIDO_COD: int})

df = df.fillna(0)
df = df[['\nCPF/CNPJ\n']].astype(int)

for i in range(0, len(df)):
    cpf_cnpj = str(df.loc[i, '\nCPF/CNPJ\n'])

    if len(cpf_cnpj) == 11:
        df_despesas.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CPF
    elif len(cpf_cnpj) > 11:
        df_despesas.loc[i, consolidacao.FAVORECIDO_TIPO] = consolidacao.TIPO_FAVORECIDO_CNPJ

print(df_despesas.head())
