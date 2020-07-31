from covidata.noticias.contratados.indice_contratados import buscar

class IdentificadorContratados:
    name = 'contratados'

    def __init__(self, busca_exata=True, num_min_caracteres=3):
        self.busca_exata = busca_exata
        self.num_min_caracteres = num_min_caracteres

    def __call__(self, doc):
        doc._.entidades_originais = doc.ents
        novas_entidades = []
        entidades_relacionadas = []

        for ent in doc._.entidades_originais:
            termo = ent.string.strip()

            if len(termo) > self.num_min_caracteres:
                if self.busca_exata:
                    termo = '"' + termo + '"'
                resultados = buscar(termo)

                if len(resultados) > 0:
                    novas_entidades.append(ent)
                    entidades_relacionadas.append(set(resultados))

        doc.ents = tuple(novas_entidades)
        doc._.entidades_relacionadas = entidades_relacionadas
        return doc