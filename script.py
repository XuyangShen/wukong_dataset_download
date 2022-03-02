
"""This is a quick script for download Noah-Wukong dataset.

@author Xuyang Shen
@date Feb 16, 2022
"""
import multiprocessing as mp
import os
import urllib.request
from subprocess import PIPE, Popen
from typing import List, Tuple

import pandas as pd
from tqdm import tqdm

DATA_FOLDER = 'wukong_release'
OUT_FOLDER = 'wukong'

MAX_CPU = max(32, mp.cpu_count()*2)


def sub_download(url, output) -> bool:
    """Backup download metod timout in 30s.

    Parameters
    ----------
    url : str

    output : str
        output path and file name

    Returns
    -------
    bool
        true if download succeeds
    """
    command = ['wget', '-c', '--timeout=30', '-O', output, url]
    try:
        p = Popen(command, stdout=PIPE, stderr=PIPE,
                  universal_newlines=True)
        out, err = p.communicate()
        rc = p.returncode
        return rc == 0
    except:
        return False



def csv_extract(csv, dir) -> Tuple[int, str, str, str, bool]:
    """Extract information from csv.

    Returns
    -------
    Tuple[str, str, str, bool]

    """
    assert os.path.exists(csv)
    
    df = pd.read_csv(csv)
    urls = df['url'].to_numpy()
    captions = df['caption'].to_numpy()

    for ind, (url, caption) in tqdm(enumerate( zip (urls, captions))):

        format = 'jpeg' if 'jepg' in url else 'jpg'
        output = os.path.join(dir, str(ind) + '.' + format)
        # enable resume
        _exists = os.path.exists(output)

        yield ind, url, caption, output, _exists


def url2file(args) -> Tuple[bool, str, str]:
    """url to files with 2 attempts.

    Parameters
    ----------
    args
        arguments from the output of "csv_extract"    

    Returns
    -------
    Tuple[int, str, str]
        status, output path/url, and caption
    """

    ind, url, caption, output, _exists = args

    if _exists:
        return -1, output, caption

    try:
        urllib.request.urlretrieve(url, output)
        return -1, output, caption
    except:
        if sub_download(url, output): 
            return -1, output, caption

    return ind, url, caption



def get_all_csv(folder) -> List:
    """Get all origianl csv paths.

    Parameters
    ----------
    folder : str
        path to the folder contains all csv files. 

    Returns
    -------
    List

    """
    assert os.path.exists(folder)
    dirs = os.listdir(folder)

    def get_file_id(s):
        return int(s.split('_')[-1][:-4])

    return sorted([os.path.join(folder, f) for f in dirs], key=get_file_id)


if __name__ == '__main__':
    process_pool = mp.Pool(MAX_CPU)

    for csv in get_all_csv(DATA_FOLDER):
        # create dic
        name = csv[:-4].split('/')[-1]
        dir = os.path.join(OUT_FOLDER, 'Data', name)
        if not os.path.exists(dir):
            os.mkdir(dir)
        
        PATH, CAPTIONS, ERROR_IND, ERROR_URL, ERROR_CAP = [], [], [], [], []
        
        print(f'{name} - start')
        for status, info, cap in process_pool.imap(url2file, csv_extract(csv, dir)):
            if status == -1:
                PATH.append(info)
                CAPTIONS.append(cap)
            else:
                ERROR_IND.append(status)
                ERROR_URL.append(info)
                ERROR_CAP.append(cap)
        
        df = pd.DataFrame({'pth': PATH, 'annoation': CAPTIONS})
        df.to_csv(os.path.join(OUT_FOLDER, 'Annotation', name + '.csv'),
                encoding='utf-8', index=None)
        # if error in download
        if len(ERROR_IND) > 0:
            df = pd.DataFrame(
                {'ind': ERROR_IND, 'url': ERROR_URL, 'annoation': ERROR_CAP})
            df.to_csv(os.path.join(OUT_FOLDER, 'Miss', name + '.csv'),
                    encoding='utf-8', index=None)

    print("Completed")
