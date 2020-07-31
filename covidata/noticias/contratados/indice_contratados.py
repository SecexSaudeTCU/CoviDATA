from whoosh import index
from whoosh.qparser import QueryParser
import pathlib

def buscar(termo):
    diretorio = pathlib.Path(__file__).parent.absolute().joinpath('indexdir')
    ix = index.open_dir(diretorio)
    qp = QueryParser("contratado_descricao", schema=ix.schema)
    q = qp.parse(termo)
    resultado = []

    with ix.searcher() as s:
        results = s.search(q)
        for result in results:
            resultado.append(result['contratado_descricao'])

    return resultado
