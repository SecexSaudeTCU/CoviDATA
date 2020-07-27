import sqlite3

from covidata import config


class DAO_RFB:
    def __init__(self):
        self.conn = sqlite3.connect(config.diretorio_base_cnpjs)

    def buscar_cnpj_por_razao_social_ou_nome_fantasia(self, nome):
        c = self.conn.cursor()
        cursor = c.execute("SELECT * FROM empresas WHERE razao_social = ? or nome_fantasia = ?", (nome, nome,))
        lista = cursor.fetchall()
        cnpjs = [elemento[0] for elemento in lista]
        return cnpjs

    def buscar_todos(self):
        c = self.conn.cursor()
        cursor = c.execute("SELECT * FROM empresas")
        return cursor

    def encerrar_conexao(self):
        self.conn.close()
