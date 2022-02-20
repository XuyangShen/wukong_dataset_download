
"""This is a quick script for download Noah-Wukong dataset.

@author Xuyang Shen
@date Feb 16, 2022
"""
import multiprocessing as mp
import os
import shutil
import time
import urllib.request
from subprocess import PIPE, Popen
from typing import List, Tuple

import pandas as pd
from tqdm import tqdm

DATA_FOLDER = 'wukong_release'
OUT_FOLDER = 'wukong'

MAX_CPU = max(mp.cpu_count(), 10)


def csv_extract(csv) -> Tuple[List, List]:
    """Extract information from csv.

    Returns
    -------
    Tuple[List, List]

    """
    assert os.path.exists(csv)

    df = pd.read_csv(csv)
    url = df['url'].to_numpy()
    caption = df['caption'].to_numpy()
    return url, caption

def download(url, output, log) -> bool:
    """Download metod with 2 attempts.

    Parameters
    ----------
    url : str

    output : str
        output path and file name
    
    log : str
        log path        

    Returns
    -------
    bool
        true if download succeeds
    """
    attemps = 2
    err = None
    while attemps > 0:
        try:
            urllib.request.urlretrieve(url, output)
            return True
        except Exception as e:
            succ = download2(url, output)
            if succ:
                return True
            err = e
            attemps -= 1
            time.sleep(0.5 * (3-attemps))

    with open(log, 'a') as f:
        f.write(str(err) + '. ' + url)
        f.write('\n')
    return False

def download2(url, output) -> bool:
    """Backup download metod.

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
    command = ['wget', '-c', '-O', output, url]
    try:
        p = Popen(command, stdout=PIPE, stderr=PIPE,
                    universal_newlines=True)
        out, err = p.communicate()
        rc = p.returncode
        return rc == 0
    except:
        return False

def run(processID:int, csv:str) -> None:
    """Start process

    Parameters
    ----------
    processID : int
        unique process id
    csv : str
        csv file
    """
    print(f'thr-{processID} start')
    # create dic
    name = csv[:-4].split('/')[-1]
    dir = os.path.join(OUT_FOLDER, 'Data', name)
    if os.path.exists(dir):
        shutil.rmtree(dir)
    os.mkdir(dir)

    # make log file
    log = os.path.join(OUT_FOLDER, 'Logs', name + '.log')
    with open(log, 'w') as _:
        pass

    urls, captions = csv_extract(csv)
    pth, new_caps, error, error_caps, error_inds = [], [], [], [], []

    for ind, (url, cap) in tqdm(enumerate(zip(urls, captions))):
        format = 'jpeg' if 'jepg' in url else 'jpg'
        succ = download(url=url, output=os.path.join(dir, str(ind) + '.' + format), log=log)
        if not succ:
            error.append(url)
            error_caps.append(cap)
            error_inds.append(ind)
        else:
            pth.append(os.path.join('Data', name, str(ind) + '.' + format))
            new_caps.append(cap)

        # sleep 5ms
        time.sleep(5/1000)

    df = pd.DataFrame({'pth': pth, 'annoation': new_caps})
    df.to_csv(os.path.join(OUT_FOLDER, 'Annotation', name + '.csv'),
                encoding='utf-8', index=None)
    # if error in download
    if len(error) > 0:
        df = pd.DataFrame(
            {'ind': error_inds, 'url': error, 'annoation': error_caps})
        df.to_csv(os.path.join(OUT_FOLDER, 'Miss', name + '.csv'),
                    encoding='utf-8', index=None)

    print(f'thr-{processID} complete {name}')


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
    csv_lst = get_all_csv(DATA_FOLDER)

    for ind, csv in enumerate(csv_lst):
        process_pool.apply_async(run, args=(ind, csv))

    process_pool.close()
    process_pool.join()
    print("Completed")
