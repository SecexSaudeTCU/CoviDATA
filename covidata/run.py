"""
Extrai os dados de todos os portais para os quais o respectivo scrapper já foi implementado e salva o resultado
na pasta dados, na raiz do projeto. Os novos scrapers devem ser adicionados ao script manualmente.
"""
import traceback
from collections import defaultdict

import datetime
import logging
import os
import pandas as pd
import sys
import time

# Adiciona diretorio raiz ao PATH. Devido a ausência de setup.py, isto garante que as importações sempre funcionarão
from covidata.consolidacao_geral import consolidar

diretorio_raiz = os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir)
sys.path.append(diretorio_raiz)

from covidata import config
from covidata.persistencia.consolidacao import salvar
from covidata.webscraping.scrappers.AC.PT_AC import PT_AC_Scraper, PT_RioBranco_Scraper
from covidata.webscraping.scrappers.AC.TCE_AC import TCE_AC_DespesasMunicipiosScraper
from covidata.webscraping.scrappers.AL.PT_AL import PT_AL_Scraper
from covidata.webscraping.scrappers.AM.PT_AM import PT_AM_Scraper, PT_Manaus_Scraper
from covidata.webscraping.scrappers.AP.PT_AP import PT_AP_Scraper, PT_Macapa_Scraper
from covidata.webscraping.scrappers.BA.PT_BA import PT_BA_Scraper
from covidata.webscraping.scrappers.BA.TCE_BA import TCE_BA_Scraper
from covidata.webscraping.scrappers.CE.PT_CE import PT_CE_Scraper, PT_Fortaleza_Scraper
from covidata.webscraping.scrappers.DF.PT_DF import PT_DF_Scraper
from covidata.webscraping.scrappers.ES.PT_ES import PT_ES_Scraper
from covidata.webscraping.scrappers.GO.PT_GO import PT_Goiania_Scraper, PT_GO_Scraper
from covidata.webscraping.scrappers.MA.PT_MA import PT_MA_Scraper, PT_SaoLuis_Scraper
from covidata.webscraping.scrappers.MA.TCE_MA import TCE_MA_Scraper
from covidata.webscraping.scrappers.MG.PT_MG import PT_MG_Scraper, PT_BeloHorizonte_Scraper
from covidata.webscraping.scrappers.MS.PT_MS import PT_MS_Scraper, PT_CampoGrande_Scraper
from covidata.webscraping.scrappers.MT.PT_MT import PT_MT_Scraper
from covidata.webscraping.scrappers.PA.PT_PA import PT_Belem_Scraper, PT_PA_Scraper
from covidata.webscraping.scrappers.PB.PT_PB import PT_PB_Scraper, PT_JoaoPessoa_Scraper
from covidata.webscraping.scrappers.PE.PT_Recife import PT_Recife_Scraper
from covidata.webscraping.scrappers.PI.TCE_PI import TCE_PI_Scraper
from covidata.webscraping.scrappers.PR.PT_PR import PT_PR_Scraper, PT_CuritibaContratacoes_Scraper, \
    PT_CuritibaAquisicoes_Scraper
from covidata.webscraping.scrappers.RJ.PT_RJ import PT_RioDeJaneiro_Favorecidos_Scraper, \
    PT_RioDeJaneiro_Contratos_Scraper, PT_RioDeJaneiro_DespesasPorAto_Scraper
from covidata.webscraping.scrappers.RJ.TCE_RJ import TCE_RJ_Scraper
from covidata.webscraping.scrappers.RN.PT_RN import PT_RN_Scraper, PT_Natal_Scraper
from covidata.webscraping.scrappers.RO.PT_RO import PT_RO_Scraper, PT_PortoVelho_Scraper
from covidata.webscraping.scrappers.RR.PT_BoaVista import PT_BoaVista_Scraper
from covidata.webscraping.scrappers.RR.PT_RR import PT_RR_Scraper
from covidata.webscraping.scrappers.RS.PT_RS import PT_RS_Scraper
from covidata.webscraping.scrappers.RS.TCE_RS import TCE_RS_Scraper
from covidata.webscraping.scrappers.SC.PT_SC import PT_SC_Scraper, PT_Florianopolis_Scraper
from covidata.webscraping.scrappers.SE.PT_SE import PT_SE_Scraper, PT_Aracaju_Scraper
from covidata.webscraping.scrappers.SP.PT_SP import PT_SP_Scraper
from covidata.webscraping.scrappers.TO.PT_TO import PT_TO_Scraper

if __name__ == '__main__':
    logger = logging.getLogger('covidata')
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    # No momento, estão nesta lista os scrapers mais instáveis, que apresentam erros intermitentes.
    map_ufs_scrapers = {
        'AC': [PT_AC_Scraper(config.url_pt_AC), PT_RioBranco_Scraper(config.url_pt_RioBranco),
               TCE_AC_DespesasMunicipiosScraper(config.url_tce_AC_despesas_municipios)],
        'AL': [PT_AL_Scraper(config.url_pt_AL)],
        'AM': [PT_AM_Scraper(config.url_pt_AM), PT_Manaus_Scraper(config.url_pt_Manaus)],
        'BA': [PT_BA_Scraper(config.url_pt_BA), TCE_BA_Scraper(config.url_tce_BA)],
        'CE': [PT_CE_Scraper(config.url_pt_CE), PT_Fortaleza_Scraper(config.url_pt_Fortaleza)],
        'AP': [PT_AP_Scraper(config.url_pt_AP), PT_Macapa_Scraper(config.url_pt_Macapa)],
        'DF': [PT_DF_Scraper(config.url_pt_DF)],
        'ES': [PT_ES_Scraper(config.url_pt_ES)],
        'GO': [PT_GO_Scraper(config.url_pt_GO), PT_Goiania_Scraper(config.url_pt_Goiania_despesas)],
        'MA': [PT_MA_Scraper(config.url_pt_MA), TCE_MA_Scraper(config.url_tce_MA),
               PT_SaoLuis_Scraper(config.url_pt_SaoLuis)],
        'MG': [PT_MG_Scraper(config.url_pt_MG), PT_BeloHorizonte_Scraper(config.url_pt_BeloHorizonte)],
        'MS': [PT_MS_Scraper(config.url_pt_MS), PT_CampoGrande_Scraper(config.url_pt_CampoGrande)],
        'MT': [PT_MT_Scraper(config.url_pt_MT)],
        'PA': [PT_PA_Scraper(config.url_pt_PA), PT_Belem_Scraper(config.url_pt_Belem)],
        'PB': [PT_PB_Scraper(config.url_pt_PB), PT_JoaoPessoa_Scraper(config.url_pt_JoaoPessoa)],
        'PE': [PT_Recife_Scraper(config.url_pt_Recife)],
        'PI': [TCE_PI_Scraper(config.url_tce_PI)],
        'PR': [PT_PR_Scraper(config.url_pt_PR), PT_CuritibaAquisicoes_Scraper(config.url_pt_Curitiba_aquisicoes),
               PT_CuritibaContratacoes_Scraper(config.url_pt_Curitiba_contratacoes)],
        'RJ': [PT_RioDeJaneiro_Favorecidos_Scraper(config.url_pt_Rio_favorecidos),
               PT_RioDeJaneiro_Contratos_Scraper(config.url_pt_Rio_contratos),
               PT_RioDeJaneiro_DespesasPorAto_Scraper(config.url_pt_Rio_despesas_por_ato),
               TCE_RJ_Scraper(config.url_tce_RJ)],
        'RN': [PT_RN_Scraper(config.url_pt_RN), PT_Natal_Scraper(config.url_pt_Natal)],
        'RO': [PT_RO_Scraper(config.url_pt_RO), PT_PortoVelho_Scraper(config.url_pt_PortoVelho)],
        'RR': [PT_RR_Scraper(config.url_pt_RR), PT_BoaVista_Scraper(config.url_pt_BoaVista)],
        'RS': [PT_RS_Scraper(config.url_pt_RS), TCE_RS_Scraper(config.url_tce_RS)],
        'SC': [PT_SC_Scraper(config.url_pt_SC_contratos), PT_Florianopolis_Scraper(config.url_pt_Florianopolis)],
        'SE': [PT_SE_Scraper(config.url_pt_SE), PT_Aracaju_Scraper(config.url_pt_Aracaju)],
        'SP': [PT_SP_Scraper(config.url_pt_SP)],
        'TO': [PT_TO_Scraper(config.url_pt_TO)],
    }
    dfs_consolidados = defaultdict(pd.DataFrame)
    erros = []

    for uf, scrapers in map_ufs_scrapers.items():
        for scraper in scrapers:
            try:
                scraper.scrap()
                data_extracao = datetime.datetime.now()
                df, salvo = scraper.consolidar(data_extracao)

                if not salvo:
                    # Se o dataframe já não foi salvo/consolidado, deverá ser salvo ao final do processamento.
                    dfs_consolidados[uf] = dfs_consolidados[uf].append(df)
            except:
                traceback.print_exc()
                erros.append(scraper.url)

    start_time = time.time()

    # TODO: Acesso disponível apenas por meio de API
    # pt_rs_capital.main() (acesso disponível apenas por meio de API)

    logger.info("--- %s minutos ---" % ((time.time() - start_time) / 60))

    # Salvando dataframes que ainda não foram salvos
    for uf, df_consolidado in dfs_consolidados.items():
        salvar(df_consolidado, uf)

    logger.info('Erros ocorridos:')
    logger.info(erros)

    #Consolidação geral
    logger.info('Consolidando todos os dados obtidos em planilha única...')
    consolidar()
