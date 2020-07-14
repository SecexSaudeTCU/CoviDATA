import datetime
import logging

from covidata import config
from covidata.municipios.ibge import get_codigo_municipio_por_nome
from covidata.persistencia import consolidacao
from os import path
import pandas as pd

from covidata.persistencia.consolidacao import consolidar_layout, salvar

#TODO: Testar planilha resultante
def __consolidar_despesas_capital(data_extracao):
    dicionario_dados = {consolidacao.GND_DESCRICAO: 'Grupo', consolidacao.MOD_APLIC_DESCRICAO: 'Modalidade',
                        consolidacao.ELEMENTO_DESPESA_COD: 'Elemento',
                        consolidacao.ELEMENTO_DESPESA_DESCRICAO: 'NomeElemento',
                        consolidacao.SUB_ELEMENTO_DESPESA_COD: 'SubElemento',
                        consolidacao.SUB_ELEMENTO_DESPESA_DESCRICAO: 'NomeSubElemento',
                        consolidacao.UG_COD: 'UG', consolidacao.UG_DESCRICAO: 'NomeUG',
                        consolidacao.CONTRATADO_DESCRICAO: 'NomeOrgao', consolidacao.CONTRATADO_CNPJ: 'Credor',
                        consolidacao.CONTRATADO_DESCRICAO: 'NomeCredor',
                        consolidacao.FONTE_RECURSOS_COD: 'FonteRecursos',
                        consolidacao.FONTE_RECURSOS_DESCRICAO: 'NomeFonteRecursos', consolidacao.FUNCAO_COD: 'Funcao',
                        consolidacao.FUNCAO_DESCRICAO: 'NomeFuncao', consolidacao.SUBFUNCAO_COD: 'SubFuncao',
                        consolidacao.SUBFUNCAO_DESCRICAO: 'NomeSubFuncao', consolidacao.PROGRAMA_COD: 'Programa',
                        consolidacao.PROGRAMA_DESCRICAO: 'NomePrograma', consolidacao.ACAO_COD: 'Acao',
                        consolidacao.ACAO_DESCRICAO: 'NomeAcao', consolidacao.VALOR_CONTRATO: 'Valor',
                        consolidacao.ANO: 'Exercicio', consolidacao.DOCUMENTO_NUMERO: 'EmpenhoExercicio',
                        consolidacao.DOCUMENTO_DATA: 'Data', consolidacao.TIPO_DOCUMENTO: 'TipoAto',
                        consolidacao.DESPESA_DESCRICAO: 'ObjetoContrato'}
    colunas_adicionais = ['Poder', 'UO', 'NomeUO', 'Orgao', 'Processo', 'Licitacao', 'Liquidacao', 'Pagamento', 'Banco',
                          'NomeBanco', 'Agencia', 'ContaCorrente', 'NomeContaCorrente', 'ASPS', 'MDE',
                          'ExercicioContrato', 'NumeroContrato', 'Historico', 'Legislacao']
    # TODO: Testar com arquivo .txt
    planilha_original = path.join(config.diretorio_dados, 'RJ', 'portal_transparencia', 'Rio de Janeiro',
                                  '_arquivos_Open_Data_Desp_Ato_Covid19_2020.csv')
    df_original = pd.read_csv(planilha_original, sep=';', encoding='ISO-8859-1')
    fonte_dados = consolidacao.TIPO_FONTE_PORTAL_TRANSPARENCIA + ' - ' + config.url_pt_Rio_despesas_por_ato
    codigo_municipio_ibge = get_codigo_municipio_por_nome('Rio de Janeiro', 'RJ')
    df = consolidar_layout(colunas_adicionais, df_original, dicionario_dados, consolidacao.ESFERA_MUNICIPAL,
                           fonte_dados, 'RJ', codigo_municipio_ibge, data_extracao)
    df[consolidacao.MUNICIPIO_DESCRICAO] = 'Rio de Janeiro'
    return df


def consolidar(data_extracao):
    logger = logging.getLogger('covidata')
    logger.info('Iniciando consolidação dados Rio de Janeiro')

    despesas_capital = __consolidar_despesas_capital(data_extracao)

    salvar(despesas_capital, 'RJ')


#consolidar(datetime.datetime.now())
