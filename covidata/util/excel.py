import os
from os import path

import win32com.client

# TODO: Solução que só funciona no Windows!
def exportar_arquivo_para_xlsx(diretorio, nome_arquivo_original, nome_novo_arquivo):
    """
    Função que só funciona no Windows, a ser utilizada exepcionalmente para arquivos XLS corruptos.  Esses arquivos não
    não abrem com o uso de bibliotecas Python convencionais. A única solução até o momento é salvar o arquivo no
    formato XLSX.
    :param diretorio: Diretório de origem e de destino.
    :param nome_arquivo_original: Nome do arquivo original.
    :param nome_novo_arquivo: Nome do arquivo a ser gerado.
    :return:
    """
    App = win32com.client.Dispatch("Excel.Application")
    App.Visible = False
    caminho_arquivo_anterior = path.join(diretorio, nome_arquivo_original)
    workbook = App.Workbooks.Open(caminho_arquivo_anterior)
    workbook.ActiveSheet.SaveAs(path.join(diretorio, nome_novo_arquivo), 51)  # 51 is for xlsx
    workbook.Close(SaveChanges=True)
    App.Quit()

    # Remove o arquivo anterior.
    os.remove(caminho_arquivo_anterior)