""" Flags.py example - sequential code """
import time 
from pathlib import Path 
from typing import Callable 

import httpx 

POP2_CC = ('CN IN US ID BR PK NG BD JP '
           'MX PH VN ET EG DE IR TR CD FR').split()

BASE_URL = 'https://www.fluentpython.com/data/flags'
DEST_DIR = Path('downloads')

def save_flag(img:bytes, filename:str) -> None:
    (DEST_DIR / filename).write_bytes(img)

def get_flag(cc: str) -> bytes:
    url = f'{BASE_URL}/{cc}/{cc}.gif'.lower()
    resp = httpx.get(url, timeout=6.1,
                     follow_redirects=True)
    resp.raise_for_status() # raises error if value isn't 2xx
    return resp.content 

def download_many(cc_list: list[str]) -> int:
    for cc in sorted(cc_list):
        image = get_flag(cc)
        save_flag(image, f'{cc}.gif')
        print(cc,end=' ',flush=True)
    return len(cc_list)

def main(downloader: Callable[[list[str]],int]) -> None:
    DEST_DIR.mkdir(exist_ok=True)
    t0 = time.perf_counter()
    count = downloader(POP2_CC)
    elapsed = time.perf_counter() - t0
    print(f'\n{count} downloads in {elapsed:.2f}s')

# if __name__ == '__main__':
#     main(download_many)

""" flags_threadpool.py example - sequential code """
from concurrent import futures 

def download_one(cc:str):
    image = get_flag(cc)
    save_flag(image, f'{cc}.gif')
    print(cc, end=' ',flush=True)
    return cc 

def download_many(cc_list: list[str]) -> int:
    with futures.ThreadPoolExecutor() as executor:
        res = executor.map(download_one, sorted(cc_list))
    
    return len(list(res))

if __name__ == '__main__':
    main(download_many)