from sortedcontainers import SortedSet
import os

from covidata import config
import pandas as pd

def consolidar():
    diretorio = os.path.join(config.diretorio_dados, 'consolidados')
    df_final = pd.DataFrame()

    for root, subdirs, files in os.walk(diretorio):
        for file in files:
            if len(file) <= 7:
                print('Lendo arquivo ' + file)
                df = pd.read_excel(os.path.join(root, file))
                df = df.drop(columns='Unnamed: 0', axis=1, errors='ignore')
                df_final = df_final.append(df)

    df_final.to_excel(os.path.join(diretorio, 'UFs.xlsx'))

consolidar()