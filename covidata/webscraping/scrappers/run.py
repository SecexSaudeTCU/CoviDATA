from webscraping.scrappers.AM import uf_am
from webscraping.scrappers.CE import uf_ce
from webscraping.scrappers.PA import uf_pa, pt_belem
from webscraping.scrappers.RJ import pt_rj_capital, tce_rj
from webscraping.scrappers.PE import pt_recife
from webscraping.scrappers.AP import uf_ap
from webscraping.scrappers.RR import uf_rr
from webscraping.scrappers.RO import uf_ro
from webscraping.scrappers.AC import uf_ac
from webscraping.scrappers.ES import uf_es
from webscraping.scrappers.SP import tcm_sp, pt_sp, pt_sp_capital
from webscraping.scrappers.AL import uf_al

if __name__ == '__main__':
    print('Recuperando dados do Acre...')
    uf_ac.main()

    print('Recuperando dados do Amapá...')
    uf_ap.main()

    print('Recuperando dados do Amazonas...')
    uf_am.main()

    print('Recuperando dados do Ceará...')
    uf_ce.main()

    print('Recuperando dados do Espírito Santo...')
    uf_es.main()

    print('Recuperando dados do Pará...')
    uf_pa.main()
    pt_belem.main()

    print('Recuperando dados de Pernambuco...')
    pt_recife.main()

    print('Recuperando dados do Rio de Janeiro...')
    #TODO: Ver como será feita a chamada abaixo
    #tce_rj.main()
    pt_rj_capital.main()

    print('Recuperando dados de Rondônia...')
    uf_ro.main()

    print('Recuperando dados de Roraima...')
    uf_rr.main()

    print('Recuperando dados de São Paulo...')
    pt_sp.main()
    pt_sp_capital.main()
    tcm_sp.main()

    print('Recuperando dados de Alagoas...')
    uf_al.main()
