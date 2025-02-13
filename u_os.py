import os
import pathlib
from zipfile import ZipFile


def get_files_list(dir):
    '''
    Сопоставить список файлов в директории
    
    :dir: путь к директории
    '''
    files_list = []
    for f in os.listdir(dir):
        if os.path.isfile(os.path.join(dir, f)):
            files_list.append(os.path.join(dir, f))
    return files_list