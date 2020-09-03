import sqlite3

from covidata import config


class DAO_RFB:
    """
    Classe de acesso a uma base que contém os dados de pessoa jurídica disponibilizados pela Receita Federal do Brasil.
    """

    def __init__(self):
        """
        Construtor da classe.
        """
        self.conn = sqlite3.connect(config.diretorio_base_cnpjs)

    def buscar_cnpj_por_razao_social(self, nome):
        c = self.conn.cursor()
        cursor = c.execute("SELECT * FROM empresas WHERE razao_social = ?", (nome,))
        lista = cursor.fetchall()
        cnpjs = [elemento[0] for elemento in lista]
        return cnpjs

    def buscar_empresa_por_razao_social(self, nome):
        """
        Busca as empresas que possuam razão social idêntica ao nome passado como parâmetro.

        :param Nome procurado.
        :return As empresas que possuam razão social idêntica ao nome passado como parâmetro.
        """
        c = self.conn.cursor()
        cursor = c.execute("SELECT * FROM empresas WHERE razao_social = ?", (nome,))
        return cursor.fetchall()

    def buscar_todos(self):
        c = self.conn.cursor()
        cursor = c.execute("SELECT * FROM empresas")
        return cursor

    def encerrar_conexao(self):
        self.conn.close()
