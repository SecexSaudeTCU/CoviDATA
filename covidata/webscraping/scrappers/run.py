from webscraping.scrappers.AM import uf_am
from webscraping.scrappers.CE import uf_ce
from webscraping.scrappers.PA import uf_pa
from webscraping.scrappers.RJ import rio_capital
from webscraping.scrappers.PE import recife
from webscraping.scrappers.AP import uf_ap
from webscraping.scrappers.RR import uf_rr
from webscraping.scrappers.RO import uf_ro
from webscraping.scrappers.AC import uf_ac
from webscraping.scrappers.ES import uf_es
from webscraping.scrappers.SP import uf_sp

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

    print('Recuperando dados de Pernambuco...')
    recife.main()

    print('Recuperando dados do Rio de Janeiro...')
    rio_capital.main()

    print('Recuperando dados de Rondônia...')
    uf_ro.main()

    print('Recuperando dados de Roraima...')
    uf_rr.main()

    print('Recuperando dados de São Paulo...')
    uf_sp.main()
