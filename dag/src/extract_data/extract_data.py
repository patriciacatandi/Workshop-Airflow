import zipfile
import requests
from io import BytesIO
import os

def run_extract_data_kaggle(url, save_path):
    # Criar diretório para salvar dados raw
    # os.makedirs(os.path.join(path,"./wids2021"), exist_ok=True)

    # Baixar dados - colocar para fazer o download > botão direito > Copiar link do download
    
    filebytes = BytesIO(
        requests.get(url).content       #para extrair somente o conteúdo
    )

    # Extrair o conteúdo do zipfile
    myzip = zipfile.ZipFile(filebytes)  # dizendo ao python que eh um arquivo zip
    myzip.extractall(save_path)      # pasta em que vamos descompactar