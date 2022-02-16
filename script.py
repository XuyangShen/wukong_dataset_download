
"""This is a quick script for download Noah-Wukong dataset.

@author Xuyang Shen
@date Feb 16, 2022
"""
import os
import queue
import shutil
import threading
import time
import urllib.request
from subprocess import PIPE, Popen
from typing import List, Tuple

import pandas as pd
from tqdm import tqdm

DATA_FOLDER = 'wukong_release'
OUT_FOLDER = 'wukong'

MAX_THREAD = 10


class wukong(threading.Thread):
    """Download wukong-dataset in multi-threads mode.

    Parameters
    ----------
    threading : _type_

    """

    def __init__(self, threadID, csv):
        """Init.

        Parameters
        ----------
        threadID : Any
            thread id for debug
        csv : _type_
            path to a specific csv file
        """
        threading.Thread.__init__(self)

        self.threadID = threadID
        self.csv = csv

    def csv_extract(self) -> Tuple[List, List]:
        """Extract information from csv.

        Returns
        -------
        Tuple[List, List]

        """
        assert os.path.exists(self.csv)

        df = pd.read_csv(self.csv)
        url = df['url'].to_numpy()
        caption = df['caption'].to_numpy()
        return url, caption

    def download(self, url, output) -> bool:
        """Download metod with 2 attempts.

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
        attemps = 2
        err = None
        while attemps > 0:
            try:
                urllib.request.urlretrieve(url, output)
                return True
            except Exception as e:
                succ = self.download2(url, output)
                if succ:
                    return True
                err = e
                attemps -= 1
                time.sleep(0.5 * (3-attemps))

        with open(self.log, 'a') as f:
            f.write(str(err) + '. ' + url)
            f.write('\n')
        return False

    def download2(self, url, output) -> bool:
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

    def run(self):
        """Start thread."""
        print(f'thr-{self.threadID} start')
        # create dic
        name = self.csv[:-4].split('/')[-1]
        dir = os.path.join(OUT_FOLDER, 'Data', name)
        if os.path.exists(dir):
            shutil.rmtree(dir)
        os.mkdir(dir)

        # make log file
        self.log = os.path.join(OUT_FOLDER, 'Logs', name + '.log')
        with open(self.log, 'w') as _:
            pass

        urls, captions = self.csv_extract()
        pth, new_caps, error, error_caps, error_inds = [], [], [], [], []

        for ind, (url, cap) in tqdm(enumerate(zip(urls, captions))):
            format = 'jpeg' if 'jepg' in url else 'jpg'
            succ = self.download(url=url, output=os.path.join(
                dir, str(ind) + '.' + format))
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

        print(f'thr-{self.threadID} complete {name}')


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
    return [os.path.join(folder, f) for f in dirs]


if __name__ == '__main__':
    threads: queue.Queue = queue.Queue()
    csv_lst = sorted(get_all_csv(DATA_FOLDER))

    for ind, csv in enumerate(csv_lst):
        threads.put(wukong(ind, csv))

    while not threads.empty():
        if len(threading.enumerate()) < MAX_THREAD:
            thr = threads.get()
            thr.start()
        else:
            time.sleep(30)

    threads.join()
    print("Completed")
