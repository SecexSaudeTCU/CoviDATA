import configparser
import io
from webdav3.client import Client

from covidata import config


def salvar(caminho_arquivo, subdiretorio, nome_arquivo='UFs.xlsx'):
    cfg = configparser.ConfigParser(interpolation=None)
    with io.open(str(config.arquivo_config_webdav), mode='r', encoding='utf-8') as fp:
        cfg.read_file(fp)

    options = {
        'webdav_hostname': cfg['webdav']['hostname'],
        'webdav_login': cfg['webdav']['login'],
        'webdav_password': cfg['webdav']['password']
    }

    pasta_virtual = cfg['webdav']['pasta_virtual']
    cliente = Client(options)
    cliente.verify = False
    lista_arquivos = cliente.list(pasta_virtual)

    if not subdiretorio in lista_arquivos:
        cliente.mkdir(pasta_virtual + subdiretorio)

    pasta_destino = pasta_virtual + subdiretorio
    cliente.upload_sync(remote_path=pasta_destino + '/' + nome_arquivo, local_path=caminho_arquivo)

    print('Upload concluído.  Listando conteúdo do diretório remoto...')
    print(cliente.list(pasta_destino))
